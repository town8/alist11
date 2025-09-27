package _123_open

import (
	"context"
	"errors"
	"fmt"
	"github.com/alist-org/alist/v3/drivers/base"
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/model"
	"github.com/alist-org/alist/v3/pkg/errgroup"
	"github.com/alist-org/alist/v3/pkg/http_range"
	"github.com/alist-org/alist/v3/pkg/utils"
	"github.com/avast/retry-go"
	"github.com/go-resty/resty/v2"
	log "github.com/sirupsen/logrus"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var (
	invalidFileNameChars = regexp.MustCompile(`[\\/:*?|><]`)
)

// UploadPartsResp 分片列表响应
type UploadPartsResp struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    struct {
		Parts []struct {
			PartNumber int64  `json:"partNumber"`
			Size       int64  `json:"size"`
			Etag       string `json:"etag"`
		} `json:"parts"`
	} `json:"data"`
}

func validateFileName(filename string) error {
	if len(filename) > 255 {
		return errors.New("文件名长度不能超过255个字符")
	}
	if strings.TrimSpace(filename) == "" {
		return errors.New("文件名不能全部是空格")
	}
	if invalidFileNameChars.MatchString(filename) {
		return errors.New("文件名不能包含以下字符: \\/:*?|><")
	}
	return nil
}

func (d *Open123) create(parentFileID int64, filename string, etag string, size int64, duplicate int, containDir bool) (*UploadCreateResp, error) {
	if err := validateFileName(filename); err != nil {
		return nil, err
	}
	
	var resp UploadCreateResp
	_, err := d.Request(UploadCreate, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"parentFileId": parentFileID,
			"filename":     filename,
			"etag":         etag,
			"size":         size,
			"duplicate":    duplicate,
			"containDir":   containDir,
		})
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *Open123) url(preuploadID string, sliceNo int64) (string, error) {
	// get upload url
	var resp UploadUrlResp
	_, err := d.Request(UploadUrl, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"preuploadId": preuploadID,
			"sliceNo":     sliceNo,
		})
	}, &resp)
	if err != nil {
		return "", err
	}
	return resp.Data.PresignedURL, nil
}

func (d *Open123) complete(preuploadID string) (*UploadCompleteResp, error) {
	var resp UploadCompleteResp
	_, err := d.Request(UploadComplete, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"preuploadId": preuploadID,
		})
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *Open123) async(preuploadID string) (*UploadAsyncResp, error) {
	var resp UploadAsyncResp
	_, err := d.Request(UploadAsync, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"preuploadId": preuploadID,
		})
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *Open123) listParts(preuploadID string) (*UploadPartsResp, error) {
	var resp UploadPartsResp
	_, err := d.Request(UploadParts, http.MethodPost, func(req *resty.Request) {
		req.SetBody(base.Json{
			"preuploadID": preuploadID,
		})
	}, &resp)
	if err != nil {
		return nil, err
	}
	return &resp, nil
}

func (d *Open123) Upload(ctx context.Context, file model.FileStreamer, createResp *UploadCreateResp, up driver.UpdateProgress) error {
	size := file.GetSize()
	chunkSize := createResp.Data.SliceSize
	uploadNums := (size + chunkSize - 1) / chunkSize
	
	// 如果文件大小大于分片大小,先获取已上传的分片列表
	if size > chunkSize {
		_, err := d.listParts(createResp.Data.PreuploadID)
		if err != nil {
			return err
		}
		// TODO: 可以在这里比对已上传的分片,实现断点续传
	}

	threadG, uploadCtx := errgroup.NewGroupWithContext(ctx, d.UploadThread,
		retry.Attempts(3),
		retry.Delay(time.Second),
		retry.DelayType(retry.BackOffDelay))
	
	for partIndex := int64(0); partIndex < uploadNums; partIndex++ {
		if utils.IsCanceled(uploadCtx) {
			break
		}

		partIndex := partIndex
		partNumber := partIndex + 1 // 分片号从1开始
		threadG.Go(func(ctx context.Context) error {
			var uploadErr error
			err := retry.Do(
				func() error {
					uploadPartUrl, err := d.url(createResp.Data.PreuploadID, partNumber)
					if err != nil {
						return err
					}

					offset := partIndex * chunkSize
					size := min(chunkSize, size-offset)
					limitedReader, err := file.RangeRead(http_range.Range{
						Start:  offset,
						Length: size})
					if err != nil {
						return err
					}

					req, err := http.NewRequestWithContext(ctx, "PUT", uploadPartUrl, limitedReader)
					if err != nil {
						return err
					}
					req = req.WithContext(ctx)
					req.ContentLength = size

					res, err := base.HttpClient.Do(req)
					if err != nil {
						return err
					}
					defer res.Body.Close()
					
					if res.StatusCode != http.StatusOK {
						return fmt.Errorf("upload part failed with status: %d", res.StatusCode)
					}

					progress := 10.0 + 85.0*float64(threadG.Success()+1)/float64(uploadNums)
					up(progress)
					return nil
				},
				retry.Attempts(3),
				retry.Delay(time.Second),
				retry.DelayType(retry.BackOffDelay),
				retry.OnRetry(func(n uint, err error) {
					log.Debugf("Retry uploading part %d: %v", partNumber, err)
				}),
			)
			if err != nil {
				uploadErr = err
			}
			return uploadErr
		})
	}

	if err := threadG.Wait(); err != nil {
		return err
	}

	// 完成上传
	uploadCompleteResp, err := d.complete(createResp.Data.PreuploadID)
	if err != nil {
		return err
	}

	// 如果不需要异步查询或已经完成
	if !uploadCompleteResp.Data.Async || uploadCompleteResp.Data.Completed {
		up(100)
		return nil
	}

	// 异步等待上传完成
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			uploadAsyncResp, err := d.async(createResp.Data.PreuploadID)
			if err != nil {
				return err
			}
			if uploadAsyncResp.Data.Completed {
				up(100)
				return nil
			}
			time.Sleep(time.Second)
		}
	}
}
