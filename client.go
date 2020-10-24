package meilisearch

// Config configure the Client
type Config struct {

	// Host is the host of your meilisearch database
	// Example: 'http://localhost:7700'
	Host string

	// APIKey is optional
	APIKey string
}

type ClientInterface interface {
	Indexes() APIIndexes
	Version() APIVersion
	Documents(indexID string) APIDocuments
	Search(indexID string) APISearch
	Updates(indexID string) APIUpdates
	Settings(indexID string) APISettings
	Keys() APIKeys
	Stats() APIStats
	Health() APIHealth
}
