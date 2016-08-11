package plugins

import (
	"sync/atomic"

	"github.com/mozilla-services/heka/message"
	. "github.com/mozilla-services/heka/pipeline"
	"github.com/newrelic-forks/go-dockerclient"
	"golang.org/x/time/rate"
)

type metric struct {
	meta            []*message.Field
	droppedMessages int64
	droppedBytes    int64
	allowedMessages int64
	allowedBytes    int64
}

type RateLimiterConfig struct {
	LimitEvery     float64  `toml:"limit_every"`
	Burst          int      `toml:"burst"`
	DockerEndpoint string   `toml:"docker_endpoint"`
	MetaFields     []string `toml:"meta_fields"`
}

type RateLimiter struct {
	conf         *RateLimiterConfig
	filterRunner FilterRunner
	pluginHelper PluginHelper
	dockerClient *docker.Client
	metrics      map[string]*metric
	last         map[string]metric
	metaFields   map[string]struct{}
	limiterMap   map[string]*rate.Limiter
}

func (r *RateLimiter) Init(config interface{}) error {
	var err error
	r.conf = config.(*RateLimiterConfig)
	r.dockerClient, err = docker.NewClient(r.conf.DockerEndpoint)
	if err != nil {
		return err
	}
	r.metrics = make(map[string]*metric)
	r.last = make(map[string]metric)
	r.metaFields = make(map[string]struct{})
	r.limiterMap = make(map[string]*rate.Limiter)
	for _, metaFieldName := range r.conf.MetaFields {
		r.metaFields[metaFieldName] = struct{}{}
	}
	return nil
}

func (r *RateLimiter) CleanupForRestart() {

}

func (r *RateLimiter) ConfigStruct() interface{} {
	return &RateLimiterConfig{
		LimitEvery: float64(5), // every 200ms
		Burst:      100,
	}
}

func (r *RateLimiter) Prepare(fr FilterRunner, h PluginHelper) (err error) {
	r.filterRunner = fr
	r.pluginHelper = h
	return nil
}

func (r *RateLimiter) CleanUp() {

}

func (r *RateLimiter) TimerEvent() (err error) {
	containers, err := r.dockerClient.ListContainers(docker.ListContainersOptions{})
	if err != nil {
		return err
	}
	activeContainers := make(map[string]struct{})
	for _, container := range containers {
		key := container.ID[:12]
		activeContainers[key] = struct{}{}
	}

	for k, v := range r.metrics {

		if _, ok := activeContainers[k]; !ok {
			delete(r.limiterMap, k)
			delete(r.metrics, k)
		} else {
			pipelinepack, err := r.pluginHelper.PipelinePack(0)
			if err != nil {
				r.filterRunner.LogError(err)
				return err
			}
			pipelinepack.Message.SetType("ThrottledLogMetrics")
			pipelinepack.Message.Fields = v.meta

			message.NewInt64Field(
				pipelinepack.Message,
				"dropped_message_count",
				v.droppedMessages,
				"count")
			message.NewInt64Field(
				pipelinepack.Message,
				"dropped_byte_count",
				v.droppedBytes,
				"count")
			message.NewInt64Field(
				pipelinepack.Message,
				"allowed_message_count",
				v.allowedMessages,
				"count")
			message.NewInt64Field(
				pipelinepack.Message,
				"allowed_byte_count",
				v.allowedBytes,
				"count")

			if last, ok := r.last[k]; ok {

				droppedMessageRate := (float64(v.droppedMessages) - float64(last.droppedMessages)) / float64(60)
				droppedByteRate := (float64(v.droppedBytes) - float64(last.droppedBytes)) / float64(60)
				allowedMessageRate := (float64(v.allowedMessages) - float64(last.allowedMessages)) / float64(60)
				allowedByteRate := (float64(v.allowedBytes) - float64(last.allowedBytes)) / float64(60)

				if field, err := message.NewField("dropped_message_rate", float64(droppedMessageRate), "RATE"); err == nil {
					pipelinepack.Message.AddField(field)
				}

				if field, err := message.NewField("dropped_byte_rate", float64(droppedByteRate), "RATE"); err == nil {
					pipelinepack.Message.AddField(field)
				}

				if field, err := message.NewField("allowed_message_rate", float64(allowedMessageRate), "RATE"); err == nil {
					pipelinepack.Message.AddField(field)
				}

				if field, err := message.NewField("allowed_byte_rate", float64(allowedByteRate), "RATE"); err == nil {
					pipelinepack.Message.AddField(field)
				}
			}
			r.last[k] = *v

			if !r.filterRunner.Inject(pipelinepack) {
				r.filterRunner.LogError(err)
				return err
			}
		}
	}

	return nil
}

func (r *RateLimiter) assignMeta(fields []*message.Field) []*message.Field {
	meta := []*message.Field{}
	for _, field := range fields {
		if _, ok := r.metaFields[field.GetName()]; ok {
			meta = append(meta, field)
		}
	}
	return meta
}

func (r *RateLimiter) ProcessMessage(pack *PipelinePack) (err error) {
	var key string

	if idVal, ok := pack.Message.GetFieldValue("ContainerID"); ok {
		key = idVal.(string)
	} else {
		r.filterRunner.LogMessage("Field 'ContainerID' not found on message.")
		return nil
	}

	_, ok := r.limiterMap[key]
	if !ok {
		meta := r.assignMeta(pack.Message.GetFields())
		r.limiterMap[key] = rate.NewLimiter(rate.Limit(rate.Limit(r.conf.LimitEvery)), r.conf.Burst)
		r.metrics[key] = &metric{meta: meta, droppedMessages: 0}
	}

	if r.limiterMap[key].Allow() {
		pipelinepack, err := r.pluginHelper.PipelinePack(pack.MsgLoopCount)
		if err != nil {
			r.filterRunner.LogError(err)
			return err
		}
		pack.Message.Copy(pipelinepack.Message)
		pipelinepack.Message.SetType("DockerLogLimited")
		if !r.filterRunner.Inject(pipelinepack) {
			r.filterRunner.LogError(err)
			return nil
		}
		atomic.AddInt64(&r.metrics[key].allowedBytes, int64(pack.Message.Size()))
		atomic.AddInt64(&r.metrics[key].allowedMessages, int64(1))

	} else {
		atomic.AddInt64(&r.metrics[key].droppedBytes, int64(pack.Message.Size()))
		atomic.AddInt64(&r.metrics[key].droppedMessages, 1)
	}

	return nil
}

func init() {
	RegisterPlugin("RateLimiterFilter", func() interface{} {
		return new(RateLimiter)
	})
}
