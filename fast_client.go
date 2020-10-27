package meilisearch

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/valyala/fasthttp"
	"log"
	"net/url"
	"time"
)

// Client is a structure that give you the power for interacting with an high-level api with meilisearch.
type FastHttpClient struct {
	config     Config
	httpClient *fasthttp.Client

	// singleton clients which don't need index id
	apiIndexes APIIndexes
	apiKeys    APIKeys
	apiStats   APIStats
	apiHealth  APIHealth
	apiVersion APIVersion
}

func (c *FastHttpClient) Indexes() APIIndexes {
	return c.apiIndexes
}
func (c *FastHttpClient) Version() APIVersion {
	return c.apiVersion
}
func (c *FastHttpClient) Documents(indexID string) APIDocuments {
	return newFastClientDocuments(c, indexID)
}
func (c *FastHttpClient) Search(indexID string) APISearch {
	return newFastClientSearch(c, indexID)
}
func (c *FastHttpClient) Updates(indexID string) APIUpdates {
	return newFastClientUpdates(c, indexID)
}
func (c *FastHttpClient) Settings(indexID string) APISettings {
	return newFastClientSettings(c, indexID)
}
func (c *FastHttpClient) Keys() APIKeys {
	return c.apiKeys
}
func (c *FastHttpClient) Stats() APIStats {
	return c.apiStats
}
func (c *FastHttpClient) Health() APIHealth {
	return c.apiHealth
}

func NewFastHttpCustomClient(config Config, client *fasthttp.Client) ClientInterface {
	c := &FastHttpClient{
		config:     config,
		httpClient: client,
	}

	c.apiIndexes = newFastClientIndexes(c)
	c.apiKeys = newFastClientKeys(c)
	c.apiHealth = newFastClientHealth(c)
	c.apiStats = newFastClientStats(c)
	c.apiVersion = newFastClientVersion(c)

	return c
}

type internalRawRequest struct {
	endpoint string
	method   string

	withRequest     json.Marshaler
	withResponse    json.Unmarshaler
	withQueryParams map[string]string

	acceptedStatusCodes []int

	functionName string
	apiName      string
}

func (c *FastHttpClient) executeRequest(req internalRawRequest) error {
	internalError := &Error{
		Endpoint:           req.endpoint,
		Method:             req.method,
		Function:           req.functionName,
		APIName:            req.apiName,
		RequestToString:    "empty request",
		ResponseToString:   "empty response",
		MeilisearchMessage: "empty meilisearch message",
		StatusCodeExpected: req.acceptedStatusCodes,
	}
	log.Printf("request %v", req)
	response, err := c.sendRequest(&req, internalError)
	if err != nil {
		return err
	}
	log.Printf("response %v", response)
	internalError.StatusCode = response.StatusCode()
	log.Printf("response code %v", response.StatusCode())
	log.Printf("response body %v", string(response.Body()))

	err = c.handleStatusCode(&req, response, internalError)
	log.Print()
	if err != nil {
		return err
	}

	err = c.handleResponse(&req, response, internalError)
	if err != nil {
		return err
	}

	return nil
}

func (c *FastHttpClient) sendRequest(req *internalRawRequest, internalError *Error) (*fasthttp.Response, error) {
	var (
		request  *fasthttp.Request
		response *fasthttp.Response
		err      error
	)

	// Setup URL
	requestURL, err := url.Parse(c.config.Host + req.endpoint)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse url")
	}

	// Build query parameters
	if req.withQueryParams != nil {
		query := requestURL.Query()
		for key, value := range req.withQueryParams {
			query.Set(key, value)
		}

		requestURL.RawQuery = query.Encode()
	}

	request = fasthttp.AcquireRequest()
	response = fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(request)
	defer fasthttp.ReleaseResponse(response)

	request.SetRequestURI(requestURL.String())
	request.Header.SetMethod(req.method)

	if req.withRequest != nil {

		// A json request is mandatory, so the request interface{} need to be passed as a raw json body.
		rawJSONRequest := req.withRequest

		data, err := rawJSONRequest.MarshalJSON()
		internalError.RequestToString = string(data)

		if err != nil {
			return nil, internalError.WithErrCode(ErrCodeMarshalRequest, err)
		}
		request.SetBody(data)
	}

	// adding request headers
	request.Header.Set("Content-Type", "application/json")
	if c.config.APIKey != "" {
		request.Header.Set("X-Meili-API-Key", c.config.APIKey)
	}

	// request is sent
	err = c.httpClient.Do(request, response)

	// request execution fail
	if err != nil {
		return nil, internalError.WithErrCode(ErrCodeRequestExecution, err)
	}

	return response, nil
}

func (c *FastHttpClient) handleStatusCode(req *internalRawRequest, response *fasthttp.Response, internalError *Error) error {
	if req.acceptedStatusCodes != nil {

		// A successful status code is required so check if the response status code is in the
		// expected status code list.
		for _, acceptedCode := range req.acceptedStatusCodes {
			if response.StatusCode() == acceptedCode {
				return nil
			}
		}

		// At this point the response status code is a failure.
		rawBody := response.Body()

		internalError.ErrorBody(rawBody)

		return internalError.WithErrCode(ErrCodeResponseStatusCode)
	}

	return nil
}

func (c *FastHttpClient) handleResponse(req *internalRawRequest, response *fasthttp.Response, internalError *Error) (err error) {
	if req.withResponse != nil {

		// A json response is mandatory, so the response interface{} need to be unmarshal from the response payload.
		rawBody := response.Body()
		internalError.ResponseToString = string(rawBody)
		if err = req.withResponse.UnmarshalJSON(rawBody); err != nil {
			return internalError.WithErrCode(ErrCodeResponseUnmarshalBody, err)
		}
	}
	return nil
}

func (c FastHttpClient) DefaultWaitForPendingUpdate(indexUID string, updateID *AsyncUpdateID) (UpdateStatus, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()

	return c.WaitForPendingUpdate(ctx, time.Millisecond*50, indexUID, updateID)
}

// WaitForPendingUpdate waits for the end of an update.
// The function will check by regular interval provided in parameter interval
// the UpdateStatus. If it is not UpdateStatusEnqueued or the ctx cancelled
// we return the UpdateStatus.
func (c FastHttpClient) WaitForPendingUpdate(
	ctx context.Context,
	interval time.Duration,
	indexID string,
	updateID *AsyncUpdateID) (UpdateStatus, error) {

	apiUpdates := c.Updates(indexID)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		update, err := apiUpdates.Get(updateID.UpdateID)
		if err != nil {
			return UpdateStatusUnknown, nil
		}
		if update.Status != UpdateStatusEnqueued {
			return update.Status, nil
		}
		time.Sleep(interval)
	}
}
