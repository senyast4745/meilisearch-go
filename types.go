package meilisearch

import (
	"bytes"
	"encoding/json"
	"github.com/valyala/fastjson"
	"log"
	"time"
)

// Unknown is unknown json type
type Unknown map[string]interface{}

//
// Internal types to Meilisearch
//

type Response interface {
	UnmarshalJSON(data []byte) error
	MarshalJSON() ([]byte, error)
}

// Index is the type that represent an index in MeiliSearch
type Index struct {
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	PrimaryKey string    `json:"primaryKey,omitempty"`
}

// Settings is the type that represents the settings in MeiliSearch
type Settings struct {
	RankingRules          []string            `json:"rankingRules,omitempty"`
	DistinctAttribute     *string             `json:"distinctAttribute,omitempty"`
	SearchableAttributes  []string            `json:"searchableAttributes,omitempty"`
	DisplayedAttributes   []string            `json:"displayedAttributes,omitempty"`
	StopWords             []string            `json:"stopWords,omitempty"`
	Synonyms              map[string][]string `json:"synonyms,omitempty"`
	AttributesForFaceting []string            `json:"attributesForFaceting,omitempty"`
}

// Version is the type that represents the versions in MeiliSearch
type Version struct {
	CommitSha  string    `json:"commitSha"`
	BuildDate  time.Time `json:"buildDate"`
	PkgVersion string    `json:"pkgVersion"`
}

// StatsIndex is the type that represent the stats of an index in MeiliSearch
type StatsIndex struct {
	NumberOfDocuments int64            `json:"numberOfDocuments"`
	IsIndexing        bool             `json:"isIndexing"`
	FieldsFrequency   map[string]int64 `json:"fieldsFrequency"`
}

// Stats is the type that represent all stats
type Stats struct {
	DatabaseSize int64                 `json:"database_size"`
	LastUpdate   time.Time             `json:"last_update"`
	Indexes      map[string]StatsIndex `json:"indexes"`
}

// UpdateStatus is the status of an update.
type UpdateStatus string

const (
	// UpdateStatusUnknown is the default UpdateStatus, should not exist
	UpdateStatusUnknown UpdateStatus = "unknown"
	// UpdateStatusEnqueued means the server know the update but didn't handle it yet
	UpdateStatusEnqueued UpdateStatus = "enqueued"
	// UpdateStatusProcessed means the server has processed the update and all went well
	UpdateStatusProcessed UpdateStatus = "processed"
	// UpdateStatusFailed means the server has processed the update and an error has been reported
	UpdateStatusFailed UpdateStatus = "failed"
)

// Update indicate information about an update
type Update struct {
	Status      UpdateStatus `json:"status"`
	UpdateID    int64        `json:"updateID"`
	Type        Unknown      `json:"type"`
	Error       string       `json:"error"`
	EnqueuedAt  time.Time    `json:"enqueuedAt"`
	ProcessedAt time.Time    `json:"processedAt"`
}

// AsyncUpdateID is returned for asynchronous method
//
// Documentation: https://docs.meilisearch.com/guides/advanced_guides/asynchronous_updates.html
type AsyncUpdateID struct {
	UpdateID int64 `json:"updateID"`
}

// Keys allow the user to connect to the MeiliSearch instance
//
// Documentation: https://docs.meilisearch.com/guides/advanced_guides/asynchronous_updates.html
type Keys struct {
	Public  string `json:"public,omitempty"`
	Private string `json:"private,omitempty"`
}

//
// Request/Response
//

// CreateIndexRequest is the request body for create index method
type CreateIndexRequest struct {
	Name       string `json:"name,omitempty"`
	UID        string `json:"uid,omitempty"`
	PrimaryKey string `json:"primaryKey,omitempty"`
}

// CreateIndexResponse is the response body for create index method
type CreateIndexResponse struct {
	Name       string    `json:"name"`
	UID        string    `json:"uid"`
	UpdateID   int64     `json:"updateID,omitempty"`
	CreatedAt  time.Time `json:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt"`
	PrimaryKey string    `json:"primaryKey,omitempty"`
}

// SearchRequest is the request url param needed for a search query.
// This struct will be converted to url param before sent.
//
// Documentation: https://docs.meilisearch.com/guides/advanced_guides/search_parameters.html
type SearchRequest struct {
	Query                 string
	Offset                int64
	Limit                 int64
	AttributesToRetrieve  []string
	AttributesToCrop      []string
	CropLength            int64
	AttributesToHighlight []string
	Filters               string
	Matches               bool
	FacetsDistribution    []string
	FacetFilters          interface{}
	PlaceholderSearch     bool
}

// SearchResponse is the response body for search method
type SearchResponse struct {
	Hits                  []interface{} `json:"hits"`
	NbHits                int64         `json:"nbHits"`
	Offset                int64         `json:"offset"`
	Limit                 int64         `json:"limit"`
	ProcessingTimeMs      int64         `json:"processingTimeMs"`
	Query                 string        `json:"query"`
	FacetsDistribution    interface{}   `json:"facetsDistribution,omitempty"`
	ExhaustiveFacetsCount interface{}   `json:"exhaustiveFacetsCount,omitempty"`
}

// ListDocumentsRequest is the request body for list documents method
type ListDocumentsRequest struct {
	Offset               int64    `json:"offset,omitempty"`
	Limit                int64    `json:"limit,omitempty"`
	AttributesToRetrieve []string `json:"attributesToRetrieve,omitempty"`
}

type RawType []byte

type Str string

type Indexes []Index

type Updates []Update

type Health struct {
	Health bool
}

type Name struct {
	Name string
}

type PrimaryKey struct {
	PrimaryKey string
}

type StrsArr []string

type Synonyms map[string][]string

type Query struct {
	Query                 string `json:"q"`
	Offset                int64
	Limit                 int64
	AttributesToRetrieve  []string
	AttributesToCrop      []string
	CropLength            int64
	AttributesToHighlight []string
	Filters               string
	Matches               bool
	FacetsDistribution    []string
	FacetFilters          interface{}
	PlaceholderSearch     bool
}

func (b *RawType) UnmarshalJSON(data []byte) error {
	*b = data
	return nil
}

func (b RawType) MarshalJSON() ([]byte, error) {
	return b, nil
}

func (i *StrsArr) UnmarshalJSON(data []byte) error {
	bf := bytes.Buffer{}
	pr := fastjson.Parser{}

	val, err := pr.ParseBytes(data)
	if err != nil {
		return err
	}
	valArray, err := val.Array()
	if err != nil {
		return err
	}
	for _, val := range valArray {
		val.MarshalTo(bf.Bytes())
		*i = append(*i, val.String())
		bf.Reset()
	}

	return nil
}

func (i StrsArr) MarshalJSON() ([]byte, error) {
	ar := fastjson.Arena{}
	indArr := ar.NewArray()
	for j, ind := range i {
		indArr.SetArrayItem(j, ar.NewString(ind))
	}
	return indArr.MarshalTo(nil), nil
}

func (i *Indexes) UnmarshalJSON(data []byte) error {
	bf := bytes.Buffer{}
	pr := fastjson.Parser{}

	log.Printf("indexes data %v", string(data))
	vals, err := pr.ParseBytes(data)
	if err != nil {
		return err
	}
	valArray, err := vals.Array()
	log.Printf("indexes data raw %v", vals.String())
	if err != nil {
		return err
	}

	for _, val := range valArray {
		ind := &Index{}
		val.MarshalTo(bf.Bytes())
		log.Printf("indexes data raw bytes %v", val.String())
		err = ind.UnmarshalJSON(bf.Bytes())
		if err != nil {
			log.Printf("parse errorororor !! %v", err)
			return err
		}
		log.Printf("indexes data raw ind %v", ind)
		*i = append(*i, *ind)
		bf.Reset()
	}

	return nil
}

func (i Indexes) MarshalJSON() ([]byte, error) {
	ar := fastjson.Arena{}
	indArr := ar.NewArray()
	for j, ind := range i {
		data, err := ind.MarshalJSON()
		if err != nil {
			return nil, err
		}
		indArr.SetArrayItem(j, ar.NewStringBytes(data))
	}
	return indArr.MarshalTo(nil), nil
}

func (u *Updates) UnmarshalJSON(data []byte) error {
	bf := bytes.Buffer{}
	pr := fastjson.Parser{}

	val, err := pr.ParseBytes(data)
	if err != nil {
		return err
	}
	valArray, err := val.Array()
	if err != nil {
		return err
	}
	upd := &Update{}
	for _, val := range valArray {
		val.MarshalTo(bf.Bytes())

		err = upd.UnmarshalJSON(bf.Bytes())
		if err != nil {
			return err
		}
		*u = append(*u, *upd)
		bf.Reset()
	}

	return nil
}

func (u Updates) MarshalJSON() ([]byte, error) {
	ar := fastjson.Arena{}
	indArr := ar.NewArray()
	for j, ind := range u {
		data, err := ind.MarshalJSON()
		if err != nil {
			return nil, err
		}
		indArr.SetArrayItem(j, ar.NewStringBytes(data))
	}
	return indArr.MarshalTo(nil), nil
}

func (s *Str) UnmarshalJSON(data []byte) error {
	str := Str(data[:])
	*s = str
	return nil
}

func (s Str) MarshalJSON() ([]byte, error) {
	return []byte(s), nil
}

func (s *Synonyms) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, s)
	return err
}

func (s Synonyms) MarshalJSON() ([]byte, error) {
	return json.Marshal(s)
}
