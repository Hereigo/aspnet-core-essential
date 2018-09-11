using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using System;
using System.Linq;

namespace GoogleCloudPubSubTest
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			// Instantiates a client
			PublisherServiceApiClient publisher = PublisherServiceApiClient.Create();

			// Your Google Cloud Platform project ID
			string projectId = "my-pubsub-test-20180911";

			string subscriptionId = "my-pubsub-test-20180911-subs";

			// The name for the new topic
			TopicName topicName = new TopicName(projectId, "New-CSharp-Topic");

			// Creates the new topic
			try
			{
				Topic topic = publisher.CreateTopic(topicName);
				Console.WriteLine($"Topic {topic.Name} created.");
			}
			catch (Grpc.Core.RpcException e)
			when (e.Status.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
			{
				Console.WriteLine($"Topic {topicName} already exists.");
			}
			// return 0;

			try
			{
				// Subscribe to the topic.
				SubscriberServiceApiClient subscriber = SubscriberServiceApiClient.Create();
				SubscriptionName subscriptionName = new SubscriptionName(projectId, subscriptionId);
				subscriber.CreateSubscription(subscriptionName, topicName, pushConfig: null, ackDeadlineSeconds: 60);

				// Publish a message to the topic.
				PubsubMessage message = new PubsubMessage
				{
					// The data is any arbitrary ByteString. Here, we're using text.
					Data = ByteString.CopyFromUtf8("Hello, Pubsub"),
					// The attributes provide metadata in a string-to-string dictionary.
					Attributes = { { "description", "Simple text message" } }
				};
				publisher.Publish(topicName, new[] { message });

				// Pull messages from the subscription. We're returning immediately, whether or not there
				// are messages; in other cases you'll want to allow the call to wait until a message arrives.
				PullResponse response = subscriber.Pull(subscriptionName, returnImmediately: true, maxMessages: 10);
				foreach (ReceivedMessage received in response.ReceivedMessages)
				{
					PubsubMessage msg = received.Message;
					Console.WriteLine($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
					Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");
				}

				// Acknowledge that we've received the messages. If we don't do this within 60 seconds (as specified
				// when we created the subscription) we'll receive the messages again when we next pull.
				subscriber.Acknowledge(subscriptionName, response.ReceivedMessages.Select(m => m.AckId));

				Console.WriteLine("\r\nShould to Delete the Topic ?");
				Console.ReadKey();

				// Tidy up by deleting the subscription and the topic.
				subscriber.DeleteSubscription(subscriptionName);
				publisher.DeleteTopic(topicName);
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
			}

			Console.WriteLine("\r\nDone.");
			Console.ReadKey();
		}
	}
}
