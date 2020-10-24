package meilisearch

import (
	"net/http"
)

type fastClientSearch struct {
	client   *FastHttpClient
	indexUID string
}

func newFastClientSearch(client *FastHttpClient, indexUID string) fastClientSearch {
	return fastClientSearch{client: client, indexUID: indexUID}
}

func (c fastClientSearch) Search(request SearchRequest) (*SearchResponse, error) {

	resp := &SearchResponse{}

	searchPostRequestParams := &Query{}

	if request.Limit == 0 {
		request.Limit = 20
	}

	if !request.PlaceholderSearch {
		searchPostRequestParams.Query = request.Query
	}
	if request.Filters != "" {
		searchPostRequestParams.Filters = request.Filters
	}
	if request.Offset != 0 {
		searchPostRequestParams.Offset = request.Offset
	}
	if request.Limit != 20 {
		searchPostRequestParams.Limit = request.Limit
	}
	if request.CropLength != 0 {
		searchPostRequestParams.CropLength = request.CropLength
	}
	if len(request.AttributesToRetrieve) != 0 {
		searchPostRequestParams.AttributesToRetrieve = request.AttributesToRetrieve
	}
	if len(request.AttributesToCrop) != 0 {
		searchPostRequestParams.AttributesToCrop = request.AttributesToCrop
	}
	if len(request.AttributesToHighlight) != 0 {
		searchPostRequestParams.AttributesToHighlight = request.AttributesToHighlight
	}
	if request.Matches {
		searchPostRequestParams.Matches = request.Matches
	}
	if len(request.FacetsDistribution) != 0 {
		searchPostRequestParams.FacetsDistribution = request.FacetsDistribution
	}
	if request.FacetFilters != nil {
		searchPostRequestParams.FacetFilters = request.FacetFilters
	}

	req := internalRawRequest{
		endpoint:            "/indexes/" + c.indexUID + "/search",
		method:              http.MethodPost,
		withRequest:         searchPostRequestParams,
		withResponse:        resp,
		acceptedStatusCodes: []int{http.StatusOK},
		functionName:        "Search",
		apiName:             "Search",
	}

	if err := c.client.executeRequest(req); err != nil {
		return nil, err
	}

	return resp, nil
}

func (c fastClientSearch) IndexID() string {
	return c.indexUID
}

func (c fastClientSearch) Client() ClientInterface {
	return c.client
}
