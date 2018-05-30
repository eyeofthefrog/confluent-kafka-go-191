package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/kelseyhightower/envconfig"
)

var config Config

type Config struct {
	Host        string `default:"127.0.0.1"`
	ZKName      string `default:"zookeeper-191"`
	ZKPort      string `default:""`
	Topic       string `default:"recreation"`
	KafkaName   string `default:"kafka-191"`
	KafkaPort   string `default:""`
	NetworkName string `default:"issue-191"`
	NetworkID   string `default:""`
}

func getConfig(app_name string, s interface{}) error {
	err := envconfig.Process(app_name, s)
	return err
}

func getPort() int {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func startZookeeperContainer(name string, port string) error {
	var err error
	var container *docker.Container
	var authConfig docker.AuthConfiguration

	repo := "solsson/zookeeper-statefulset"
	tag := "3.4.9"
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	exposedPort := map[docker.Port]struct{}{
		"2181/tcp": {},
	}

	createContConf := docker.Config{
		ExposedPorts: exposedPort,
		Image:        fmt.Sprintf("%s:%s", repo, tag),
	}

	portBindings := map[docker.Port][]docker.PortBinding{
		"2081/tcp": {{HostIP: "", HostPort: port}},
	}

	networkingConfig := docker.NetworkingConfig{
		EndpointsConfig: map[string]*docker.EndpointConfig{
			config.NetworkName: &docker.EndpointConfig{
				NetworkID: config.NetworkID,
			},
		},
	}

	createContHostConfig := docker.HostConfig{
		PortBindings:    portBindings,
		PublishAllPorts: false,
		Privileged:      false,
	}

	createContOps := docker.CreateContainerOptions{
		Name:             name,
		Config:           &createContConf,
		HostConfig:       &createContHostConfig,
		NetworkingConfig: &networkingConfig,
	}

	pullImageOps := docker.PullImageOptions{
		Repository: repo,
		Tag:        tag,
	}

	fmt.Println("Pulling Zookeeper Docker image.")
	if err = client.PullImage(pullImageOps, authConfig); err != nil {
		return err
	}

	fmt.Println("Creating Zookeeper container.")
	container, err = client.CreateContainer(createContOps)
	if err != nil {
		return err
	}

	fmt.Println("Starting Zookeeper container.")
	if err = client.StartContainer(container.ID, nil); err != nil {
		return err
	}

	return nil
}

func startKafkaContainer(name string, port string, zookeeper string) error {
	var err error
	var container *docker.Container
	var authConfig docker.AuthConfiguration

	repo := "solsson/kafka"
	tag := "1.0.0"
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	exposedPort := map[docker.Port]struct{}{
		"9092/tcp": {},
	}

	var command bytes.Buffer
	command.WriteString("./bin/kafka-server-start.sh")
	command.WriteString(" config/server.properties")
	command.WriteString(" --override broker.id=0")
	command.WriteString(" --override auto.create.topics.enable=true")
	command.WriteString(" --override num.partitions=1")
	command.WriteString(" --override log.dirs=/opt/kafka/data/logs")
	command.WriteString(" --override socket.request.max.bytes=318762506")
	command.WriteString(" --override log.roll.ms=30000")
	command.WriteString(fmt.Sprintf(" --override listeners=PLAINTEXT://%s:9092", name))
	command.WriteString(fmt.Sprintf(" --override advertised.listeners=PLAINTEXT://%s:9092", name))
	command.WriteString(fmt.Sprintf(" --override zookeeper.connect=%s", zookeeper))

	createContConf := docker.Config{
		ExposedPorts: exposedPort,
		Image:        fmt.Sprintf("%s:%s", repo, tag),
		Entrypoint:   []string{"sh", "-c", command.String()},
	}

	portBindings := map[docker.Port][]docker.PortBinding{
		"9092/tcp": {{HostIP: "", HostPort: port}},
	}

	networkingConfig := docker.NetworkingConfig{
		EndpointsConfig: map[string]*docker.EndpointConfig{
			config.NetworkName: &docker.EndpointConfig{
				NetworkID: config.NetworkID,
			},
		},
	}

	createContHostConfig := docker.HostConfig{
		PortBindings:    portBindings,
		PublishAllPorts: false,
		Privileged:      false,
	}

	createContOps := docker.CreateContainerOptions{
		Name:             name,
		Config:           &createContConf,
		HostConfig:       &createContHostConfig,
		NetworkingConfig: &networkingConfig,
	}

	pullImageOps := docker.PullImageOptions{
		Repository: repo,
		Tag:        tag,
	}

	fmt.Println("Pulling Kafka Docker image.")
	if err = client.PullImage(pullImageOps, authConfig); err != nil {
		return err
	}

	fmt.Println("Creating Kafka container.")
	container, err = client.CreateContainer(createContOps)
	if err != nil {
		return err
	}

	fmt.Println("Starting Kafka container.")
	if err = client.StartContainer(container.ID, nil); err != nil {
		return err
	}

	return nil
}

func destroyDockerContainer(containerID string) error {
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	fmt.Printf("Stopping container %s.\n", containerID)
	err := client.StopContainer(containerID, 30)

	fmt.Printf("Removing container %s.\n", containerID)
	err = client.RemoveContainer(docker.RemoveContainerOptions{
		ID:            containerID,
		RemoveVolumes: true,
		Force:         true,
	})

	return err
}

func getContainer(name string) (*docker.APIContainers, error) {
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	containers, err := client.ListContainers(docker.ListContainersOptions{All: true})
	if err != nil {
		return nil, err
	}

	for _, i := range containers {
		for _, n := range i.Names {
			if n == fmt.Sprintf("/%s", name) {
				return &i, nil
			}
		}
	}
	return nil, errors.New("no container found")
}

func createNetwork(name string) (*docker.Network, error) {
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	fmt.Printf("Creating network %s.\n", name)
	return client.CreateNetwork(docker.CreateNetworkOptions{
		Name: name,
	})
}

func deleteNetwork(name string) error {
	endpoint := "unix:///var/run/docker.sock"
	client, _ := docker.NewClient(endpoint)

	networks, err := client.ListNetworks()
	if err != nil {
		return err
	}

	for _, n := range networks {
		if n.Name == name {
			fmt.Printf("Removing network %s.\n", name)
			err = client.RemoveNetwork(n.ID)
			break
		}
	}

	return err
}

func setup() error {
	var err error

	if _, err = createNetwork(config.NetworkName); err != nil {
		fmt.Printf("Error creating the network: %v", err)
		return err
	}

	config.ZKPort = strconv.Itoa(getPort())

	err = startZookeeperContainer(config.ZKName, config.ZKPort)
	if err != nil {
		fmt.Printf("Error starting container: %v\n", err)
		return err
	}

	config.KafkaPort = strconv.Itoa(getPort())

	err = startKafkaContainer(config.KafkaName, config.KafkaPort, fmt.Sprintf("%s:2181", config.ZKName))
	if err != nil {
		fmt.Printf("Error starting container: %v\n", err)
		return err
	}

	return nil
}

func tearDown() error {
	apiContainer, err := getContainer(config.ZKName)
	if err != nil {
		return err
	}

	if err = destroyDockerContainer(apiContainer.ID); err != nil {
		fmt.Printf("Unable to stop container: %v\n", err)
		return err
	}

	apiContainer, err = getContainer(config.KafkaName)
	if err != nil {
		return err
	}

	if err = destroyDockerContainer(apiContainer.ID); err != nil {
		fmt.Printf("Unable to stop container: %v\n", err)
		return err
	}

	if err = deleteNetwork(config.NetworkName); err != nil {
		fmt.Printf("Unable to delete network: %v\n", err)
	}

	return nil
}

func performRecreation() error {

	fmt.Print("Press Enter to continue: ")
	bufio.NewReader(os.Stdin).ReadBytes('\n')

	return nil
}

func main() {
	var err error
	err = getConfig("confluent-kafka-go-191", &config)
	if err != nil {
		fmt.Printf("Error getting config values: %v\n", err)
		return
	}

	err = setup()
	if err != nil {
		tearDown()
		fmt.Printf("Error during setup.\n")
		return
	}

	time.Sleep(20 * time.Second)

	if err = performRecreation(); err != nil {
		fmt.Printf("Error performing recreation: %v\n", err)
		tearDown()
		return
	}

	err = tearDown()
	if err != nil {
		fmt.Printf("Error during cleanup: %v\n", err)
		return
	}
}
