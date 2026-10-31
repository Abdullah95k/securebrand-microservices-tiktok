package topics

// ValueSchemas maps each public topic to the proto file (import path in
// api/proto) declaring its value message. Records carry the message type in
// the igs-type header and plain protobuf bytes; the Schema Registry subjects
// (<topic>-value) document and gate the contract without changing the wire
// format.
var ValueSchemas = map[string]string{
	Page:          "securebrand/fb/v1/page.proto",
	Post:          "securebrand/fb/v1/post.proto",
	PostMetrics:   "securebrand/fb/v1/post.proto",
	Comment:       "securebrand/fb/v1/comment.proto",
	Media:         "securebrand/fb/v1/media.proto",
	MediaReady:    "securebrand/fb/v1/media.proto",
	MediaProcessd: "securebrand/fb/v1/media.proto",
	MediaDeleted:  "securebrand/fb/v1/media.proto",
	Job:           "securebrand/fb/v1/job.proto",
	Erasure:       "securebrand/fb/v1/erasure.proto",
}
