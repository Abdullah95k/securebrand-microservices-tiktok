package content

// The contract's topics tks publishes on (registry/topics.yaml). They are
// shared with the other collectors: the platform creates them in
// production, and tks's own topic list (configs/tt/topics.yaml) does not
// hold them. Tests and the local stack create them with Topics.
const (
	topicAccount         = "content.account.v1"
	topicAccountSnapshot = "content.account_snapshot.v1"
	topicPost            = "content.post.v1"
	topicComment         = "content.comment.v1"
	topicMetrics         = "content.metrics.v1"
	topicMedia           = "content.media.v1"
	topicAudio           = "content.audio.v1"

	// The media hand-off (M15b): tks-media-downloader announces each
	// stored file; the consumers acknowledge it, and its sweeper deletes
	// it and says so (the ledger reads both).
	TopicMediaReady     = "content.media.ready.v1"
	TopicMediaProcessed = "content.media.processed.v1"
	TopicMediaDeleted   = "content.media.deleted.v1"
	// TopicCoverage carries one JobCoverage per finished job, beside
	// tt.job.v1 (the scheduler's finalizer and reaper).
	TopicCoverage = "collection.coverage.v1"
	// TopicCandidate carries discovery's found accounts and their outcomes
	// (the scheduler's candidate publisher).
	TopicCandidate = "discovery.candidate.v1"
)

// Topics lists the content.* topics the persister publishes on.
var Topics = []string{topicAccount, topicAccountSnapshot, topicPost, topicComment, topicMetrics, topicMedia, topicAudio}

// AllTopics lists every shared topic tks publishes on or reads: the
// persister's, the media hand-off's, coverage and candidates. The platform
// makes them in production; tests and the local stack make them here.
var AllTopics = append(append([]string{}, Topics...),
	TopicMediaReady, TopicMediaProcessed, TopicMediaDeleted, TopicCoverage, TopicCandidate)

// Headers (the contract's README §11): the full message name and the
// contract's version.
const (
	HeaderType     = "sb-type"
	HeaderContract = "sb-contract"
)
