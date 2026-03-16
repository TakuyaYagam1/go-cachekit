package cache

import "context"

// PubSubStore provides Publish and Subscribe for a channel.
type PubSubStore interface {
	// Publish sends message to channel.
	Publish(ctx context.Context, channel, message string) error
	// Subscribe returns a channel that receives messages for the given channel. Cancel ctx to release resources and stop receiving.
	Subscribe(ctx context.Context, channel string) (<-chan string, error)
}
