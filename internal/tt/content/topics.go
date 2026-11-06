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
)

// Topics lists the content.* topics the persister publishes on.
var Topics = []string{topicAccount, topicAccountSnapshot, topicPost, topicComment, topicMetrics, topicMedia, topicAudio}

// Headers (the contract's README §11): the full message name and the
// contract's version.
const (
	HeaderType     = "sb-type"
	HeaderContract = "sb-contract"
)
