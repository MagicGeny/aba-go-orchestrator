package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Store struct {
	client            *s3.Client
	bucket            string
	publicBaseURL     string
	endpoint          string
	fidUploadURL      string
	httpClient        *http.Client
	useS3SignedPut    bool
	useSeaweedHTTPFid bool
	httpPublicBaseURL string
}

type S3StoreConfig struct {
	Endpoint      string
	Region        string
	Bucket        string
	AccessKey     string
	SecretKey     string
	PublicBaseURL string
	// FIDUploadURL is optional SeaweedFS volume-server URL for direct HTTP upload
	// (e.g., http://seaweedfs-volume:8080/submit or http://localhost:8080/submit).
	// When set and S3 PutObject fails or credentials are anonymous, the store will
	// fall back to SeaweedFS HTTP multipart upload.
	FIDUploadURL     string
	UseS3SignedPut   bool
	UseSeaweedFID    bool
	SeaweedSubmitURL string
	ForceHTTPUpload  bool
	// HTTPPublicBaseURL is the public URL prefix used to build downloadable URLs
	// from SeaweedFS volume /submit responses (e.g. http://localhost:8333).
	// Defaults to Endpoint.
	HTTPPublicBaseURL string
}

func NewS3Store(ctx context.Context, cfg S3StoreConfig) (*S3Store, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket is required")
	}

	endpoint := strings.TrimRight(cfg.Endpoint, "/")

	var creds aws.CredentialsProvider
	if cfg.AccessKey == "" && cfg.SecretKey == "" {
		creds = aws.AnonymousCredentials{}
	} else {
		creds = credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
	}

	resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...any) (aws.Endpoint, error) {
		if service == s3.ServiceID {
			return aws.Endpoint{
				URL:               endpoint,
				SigningRegion:     cfg.Region,
				HostnameImmutable: true,
			}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(creds),
		config.WithEndpointResolverWithOptions(resolver),
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	publicBaseURL := cfg.PublicBaseURL
	if publicBaseURL == "" {
		publicBaseURL = endpoint
	}

	return &S3Store{
		client:            client,
		bucket:            cfg.Bucket,
		publicBaseURL:     strings.TrimRight(publicBaseURL, "/"),
		endpoint:          endpoint,
		fidUploadURL:      strings.TrimRight(cfg.SeaweedSubmitURL, "/"),
		httpClient:        &http.Client{Timeout: 10 * time.Minute},
		useS3SignedPut:    cfg.UseS3SignedPut,
		useSeaweedHTTPFid: cfg.ForceHTTPUpload || cfg.SeaweedSubmitURL != "" || cfg.FIDUploadURL != "",
	}, nil
}

func isRetryableS3Err(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "accessdenied") || strings.Contains(s, "403") || strings.Contains(s, "notimplemented") || strings.Contains(s, "501")
}

func (s *S3Store) Put(ctx context.Context, key string, body io.Reader, contentType string) (string, error) {
	// Buffer body since we need to be able to retry via HTTP fallback.
	buf, err := io.ReadAll(body)
	if err != nil {
		return "", fmt.Errorf("read attachment body: %w", err)
	}

	// If HTTP-only mode is forced, skip S3 entirely.
	if s.fidUploadURL != "" && !s.useS3SignedPut {
		return s.putSeaweedHTTP(ctx, key, buf, contentType)
	}

	var s3Err error
	if !s.useSeaweedHTTPFid || s.useS3SignedPut {
		_, s3Err = s.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(s.bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(buf),
			ContentType: aws.String(contentType),
		})
		if s3Err == nil {
			u, perr := url.Parse(s.publicBaseURL)
			if perr != nil {
				return "", perr
			}
			u.Path = strings.TrimRight(u.Path, "/") + "/" + s.bucket + "/" + key
			return u.String(), nil
		}
		if !isRetryableS3Err(s3Err) {
			return "", s3Err
		}
	}

	if s.useSeaweedHTTPFid && s.fidUploadURL != "" {
		publicURL, hErr := s.putSeaweedHTTP(ctx, key, buf, contentType)
		if hErr == nil {
			return publicURL, nil
		}
		if s3Err != nil {
			return "", fmt.Errorf("s3 put failed (%v); seaweed fallback failed (%w)", s3Err, hErr)
		}
		return "", hErr
	}

	if s3Err != nil {
		return "", s3Err
	}
	return "", fmt.Errorf("no upload configured")
}

func (s *S3Store) putSeaweedHTTP(ctx context.Context, key string, data []byte, contentType string) (string, error) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	part, werr := w.CreateFormFile("file", key)
	if werr != nil {
		return "", werr
	}
	if _, werr = part.Write(data); werr != nil {
		return "", werr
	}
	if cerr := w.Close(); cerr != nil {
		return "", cerr
	}

	req, rerr := http.NewRequestWithContext(ctx, http.MethodPost, s.fidUploadURL, &body)
	if rerr != nil {
		return "", rerr
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	log.Printf("[attachment-store] POST %s (key=%s, bytes=%d, contentType=%s)", s.fidUploadURL, key, len(data), contentType)

	resp, doErr := s.httpClient.Do(req)
	if doErr != nil {
		log.Printf("[attachment-store] HTTP error: %v", doErr)
		return "", doErr
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	log.Printf("[attachment-store] response status=%d body=%s", resp.StatusCode, string(respBody))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("seaweedfs upload failed: status=%d body=%s", resp.StatusCode, string(respBody))
	}

	// SeaweedFS volume "submit" typically returns JSON like:
	// {"eTag":"...","fid":"3,01234567","fileName":"<key>","fileSize":N,...}
	// SeaweedFS filer multipart upload returns JSON like:
	// {"name":"<key>","size":N,"eTag":"...","url":"http://..."}
	// The "url" field returned by the filer usually points to the volume server
	// from the filer's perspective (e.g. http://127.0.0.1:8080/...) and may
	// not be reachable from the worker. We therefore build the public URL
	// ourselves and ALWAYS emit an absolute URL with scheme+host.
	//
	// IMPORTANT: In ForceHTTPUpload mode, files are stored on the FILER, not the
	// S3 API. The filer serves them as plain HTTP on its own port (8888 by
	// default), while the S3 API (8333) is a separate, gated endpoint that
	// requires auth. We must therefore prefer the FILER host over the S3
	// endpoint when building downloadable URLs in this mode.
	publicBase := s.httpPublicBaseURL
	if publicBase == "" {
		if s.useSeaweedHTTPFid && !s.useS3SignedPut {
			publicBase = stripPath(s.fidUploadURL)
		} else {
			publicBase = s.endpoint
		}
	}
	publicBase = strings.TrimRight(publicBase, "/")

	var m map[string]any
	if jerr := json.Unmarshal(respBody, &m); jerr == nil {
		if fid, _ := m["fid"].(string); fid != "" {
			final := publicBase + "/" + fid
			log.Printf("[attachment-store] public URL (from fid): %s", final)
			return final, nil
		}
		if name, _ := m["name"].(string); name != "" {
			final := publicBase + "/" + s.bucket + "/" + name
			log.Printf("[attachment-store] public URL (from name): %s", final)
			return final, nil
		}
		if u, _ := m["url"].(string); u != "" {
			// Rebuild from public base to avoid using the filer's internal URL.
			rebuilt := publicBase + "/" + s.bucket + "/" + key
			log.Printf("[attachment-store] filer returned url=%s; rebuilding as %s", u, rebuilt)
			return rebuilt, nil
		}
	}

	// Fallback: try to parse the raw body as a plain URL string.
	raw := strings.TrimSpace(string(respBody))
	raw = strings.Trim(raw, "\"'`")
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		return raw, nil
	}

	return "", fmt.Errorf("seaweedfs upload returned unexpected body: %s", string(respBody))
}

// stripPath returns the scheme + host[:port] portion of rawURL.
// Example: "http://localhost:8888/campaign-attachments" -> "http://localhost:8888"
func stripPath(rawURL string) string {
	if rawURL == "" {
		return ""
	}
	// Find the "://" separator.
	schemeIdx := strings.Index(rawURL, "://")
	if schemeIdx < 0 {
		return rawURL
	}
	rest := rawURL[schemeIdx+3:]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' || rest[i] == '?' || rest[i] == '#' {
			return rawURL[:schemeIdx+3+i]
		}
	}
	return rawURL
}
