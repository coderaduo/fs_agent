package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"strings"

	"github.com/0x19/goesl"

	//"github.com/aws/aws-sdk-go-v2/aws"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go/aws"
)

var (
	fshost   = flag.String("fshost", "localhost", "Freeswitch hostname. Default: localhost")
	fsport   = flag.Uint("fsport", 8021, "Freeswitch port. Default: 8021")
	password = flag.String("pass", "ClueCon", "Freeswitch password. Default: ClueCon")
	timeout  = flag.Int("timeout", 10, "Freeswitch conneciton timeout in seconds. Default: 10")
	region = flag.String("region", "", "Freeswitch region. Default: empty")
)

type SQSSendMessageAPI interface {
	GetQueueUrl(ctx context.Context,
		params *sqs.GetQueueUrlInput,
		optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)

	SendMessage(ctx context.Context,
		params *sqs.SendMessageInput,
		optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
}

func GetQueueURL(c context.Context, api SQSSendMessageAPI, input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	return api.GetQueueUrl(c, input)
}

func SendMsg(c context.Context, api SQSSendMessageAPI, input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return api.SendMessage(c, input)
}

func main() {

	systemEvents := map[string]bool{"RE_SCHEDULE": true, "HEARTBEAT": true} //Should handled in the agent side

	queue := flag.String("q", "fs_events", "The name of the queue")
	flag.Parse()

	if *queue == "" {
		fmt.Println("You must supply the name of a queue (-q QUEUE)")
		return
	}

	
	// this is the region based config loader for testing
	// cfg, err := config.LoadDefaultConfig(
	// 	context.TODO(),
	// 	config.WithRegion("us-east-1"),
	// )

	cfg, conErr := config.LoadDefaultConfig(
		context.TODO(),
	)

	if len(*region) > 0 {
		cfg, conErr = config.LoadDefaultConfig(
		context.TODO(),
		config.WithRegion(*region),)
	}

	fmt.Println(cfg)
	if conErr != nil {
		panic("configuration error, " + conErr.Error())
	}

	sqs_client := sqs.NewFromConfig(cfg)
	gQInput := &sqs.GetQueueUrlInput{
		QueueName: queue,
	}

	result, err := GetQueueURL(context.TODO(), sqs_client, gQInput)
	if err != nil {
		fmt.Println("Got an error getting the queue URL:")
		fmt.Println(err)
		return
	}

	queueURL := result.QueueUrl

	// Boost it as much as it can go ...
	runtime.GOMAXPROCS(runtime.NumCPU())

	fs_client, err := goesl.NewClient(*fshost, *fsport, *password, *timeout)

	if err != nil {
		goesl.Error("Error while creating new client: %s", err)
		return
	}

	goesl.Debug("Yuhu! New client: %q", fs_client)

	// Apparently all is good... Let us now handle connection :)
	// We don't want this to be inside of new connection as who knows where it my lead us.
	// Remember that this is crutial part in handling incoming messages :)
	go fs_client.Handle()

	fs_client.Send("events json ALL")

	//client.BgApi(fmt.Sprintf("originate %s %s", "sofia/internal/1001@127.0.0.1", "&socket(192.168.1.2:8084 async full)"))

	for {
		msg, err := fs_client.ReadMessage()

		if err != nil {

			// If it contains EOF, we really dont care...
			if !strings.Contains(err.Error(), "EOF") && err.Error() != "unexpected end of JSON input" {
				goesl.Error("Error while reading Freeswitch message: %s", err)
			}
			break
		}

		data, _ := json.Marshal(msg.Headers)

		goesl.Notice("%s", msg.Headers["Event-Name"])
		evName, evNameFound := msg.Headers["Event-Name"]
		evTime, evTimeFound := msg.Headers["Event-Date-Timestamp"]
		swName, swNameFound := msg.Headers["FreeSWITCH-Switchname"]

		isSystem, isSystemFound := systemEvents[evName]

		if isSystemFound && isSystem{
			fmt.Printf("The event %s is a system event -> Not sending to sqs", evName)
			continue
		}

		if  evNameFound &&  evTimeFound && swNameFound{ 
			go func(){
				sMInput := &sqs.SendMessageInput{
					DelaySeconds: 10,
					MessageAttributes: map[string]types.MessageAttributeValue{
						"EventName": {
							DataType:    aws.String("String"),
							StringValue: aws.String(evName),
						},
						"EventDateTimestamp": {
							DataType:    aws.String("String"),
							StringValue: aws.String(evTime),
						},
						"Switchname": {
								DataType:    aws.String("String"),
								StringValue: aws.String(swName),
						},
					},
					MessageBody: aws.String(string(data)),
					QueueUrl:    queueURL,
				}

				resp, err := SendMsg(context.TODO(), sqs_client, sMInput)
				if err != nil {
					fmt.Println("Got an error sending the message:")
					fmt.Println(err)
					return
				}

				fmt.Println("Sent message with ID: " + *resp.MessageId)
			}()
		}else{
			fmt.Println("No enough data found to process the event ... ", evName)
		}

	}
}