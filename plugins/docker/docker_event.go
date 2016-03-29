package docker

import (
	"fmt"
	"time"

	"github.com/fsouza/go-dockerclient"
	"github.com/mozilla-services/heka/message"
	"github.com/mozilla-services/heka/pipeline"
	"github.com/pborman/uuid"
)

type DockerEventInputConfig struct {
	Endpoint string `toml:"endpoint"`
	CertPath string `toml:"cert_path"`
}

type DockerEventInput struct {
	conf         *DockerEventInputConfig
	dockerClient DockerClient
	eventStream  chan *docker.APIEvents
	stopChan     chan error
}

func (dei *DockerEventInput) ConfigStruct() interface{} {
	return &DockerEventInputConfig{
		Endpoint: "unix:///var/run/docker.sock",
		CertPath: "",
	}
}

func (dei *DockerEventInput) Init(config interface{}) error {
	dei.conf = config.(*DockerEventInputConfig)
	c, err := newDockerClient(dei.conf.CertPath, dei.conf.Endpoint)
	if err != nil {
		return fmt.Errorf("DockerEventInput: failed to attach to docker event API: %s", err.Error())
	}

	dei.dockerClient = c
	dei.eventStream = make(chan *docker.APIEvents)
	dei.stopChan = make(chan error)

	err = dei.dockerClient.AddEventListener(dei.eventStream)
	if err != nil {
		return fmt.Errorf("DockerEventInput: failed to add event listener: %s", err.Error())
	}
	return nil
}

func (dei *DockerEventInput) Run(ir pipeline.InputRunner, h pipeline.PluginHelper) error {
	defer dei.dockerClient.RemoveEventListener(dei.eventStream)
	defer close(dei.eventStream)
	var (
		ok   bool
		err  error
		pack *pipeline.PipelinePack
	)
	hostname := h.Hostname()

	// Provides empty PipelinePacks
	packSupply := ir.InChan()

	ok = true
	for ok {
		select {
		case event := <-dei.eventStream:
			pack = <-packSupply
			pack.Message.SetType("DockerEvent")
			pack.Message.SetLogger(event.ID)
			pack.Message.SetHostname(hostname)

			payload := fmt.Sprintf("%s %s %s", event.Action, event.Type, event.Actor.Attributes["Image"])
			pack.Message.SetPayload(payload)
			if event.TimeNano == 0 {
				pack.Message.SetTimestamp(time.Now().UnixNano())
			} else {
				pack.Message.SetTimestamp(event.TimeNano)
			}
			pack.Message.SetUuid(uuid.NewRandom())
			message.NewStringField(pack.Message, "action", event.Action)
			message.NewStringField(pack.Message, "type", event.Type)
			message.NewStringField(pack.Message, "id", event.Actor.ID)
			for k, v := range event.Actor.Attributes {
				message.NewStringField(pack.Message, k, v)
			}
			ir.Deliver(pack)
		case err = <-dei.stopChan:
			ok = false
		}
	}
	return err
}

func (dei *DockerEventInput) Stop() {
	close(dei.stopChan)
}

func (dei *DockerEventInput) CleanupForRestart() {
	// Intentially left empty. Cleanup happens in Run()
}

func init() {
	pipeline.RegisterPlugin("DockerEventInput", func() interface{} {
		return new(DockerEventInput)
	})
}
