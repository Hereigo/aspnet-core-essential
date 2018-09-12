using Google.Cloud.PubSub.V1;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace GoogleCloudPubSubTest
{
	internal class Program
	{
		// Your Google Cloud Platform project ID
		private const string projectId = "my-pubsub-test-20180911";

		private static readonly string topicName = $"Topic-test-{DateTime.Now.ToString("yyyyMMdd")}";
		private static readonly string subscriptionId = $"Subs-test-{DateTime.Now.ToString("yyyyMMdd")}";

		private static void Main(string[] args)
		{
			Environment.SetEnvironmentVariable("GOOGLE_APPLICATION_CREDENTIALS", @"D:\My_PubSub_Test_20180911-c58600a569b5.json");

			Task.Run(() => Start());

			Console.WriteLine("Service has started...");

			Console.ReadKey();
		}

		private static void Start()
		{
			SubscriberClient _subscriber;
			PublisherClient _publisher;
			// Instantiates a client
			PublisherServiceApiClient publisherApi = PublisherServiceApiClient.Create();
			// Subscribe to the topic.
			TopicName pubsubTopicName = new TopicName(projectId, topicName);
			SubscriptionName subscriptionName = new SubscriptionName(projectId, subscriptionId);
			SubscriberServiceApiClient subscriberApi = SubscriberServiceApiClient.Create();

			// Creates the new topic
			try
			{
				Topic topic = publisherApi.CreateTopic(pubsubTopicName);
				Console.WriteLine($"Topic {topic.Name} created.");
			}
			catch (Grpc.Core.RpcException e)
			when (e.Status.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
			{
				Console.WriteLine($"Topic {topicName} already exists.");
			}
			// Create the new subscription
			try
			{
				subscriberApi.CreateSubscription(subscriptionName, pubsubTopicName, null, 120);
				Console.WriteLine($"Subscription {subscriptionName.Kind} created.");
			}
			catch (Grpc.Core.RpcException e) when (e.Status.StatusCode == Grpc.Core.StatusCode.AlreadyExists)
			{
				// OK
				Console.WriteLine($"Subscription {subscriptionName.Kind} already exists");
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex);
			}

			_subscriber = SubscriberClient.Create(subscriptionName, new[] { subscriberApi });

			_publisher = PublisherClient.Create(pubsubTopicName, new[] { publisherApi });

			_publisher.PublishAsync("Bla-Bla-Bla-Message.");

			_subscriber.StartAsync((message, token) =>
			{
				string data = message.Data.ToStringUtf8();
				try
				{
					Console.WriteLine($"Pubsub message id={message.MessageId}, " +
						$"created at {message.PublishTime}, data{message.Data.ToStringUtf8()}");

					// TODO: Replace with ACK
					return System.Threading.Tasks.Task.FromResult(SubscriberClient.Reply.Nack);
				}
				catch (Exception ex)
				{
					Console.WriteLine(ex.Message);
					return System.Threading.Tasks.Task.FromResult(SubscriberClient.Reply.Nack);
				}
			});

			// VARIAN II :

			// Pull messages from the subscription.We're returning immediately, whether or not there
			// are messages; in other cases you'll want to allow the call to wait until a message arrives.
			PullResponse response = subscriberApi.Pull(subscriptionName, returnImmediately: true, maxMessages: 10);

			foreach (ReceivedMessage received in response.ReceivedMessages)
			{
				PubsubMessage msg = received.Message;
				Console.WriteLine($"Received message {msg.MessageId} published at {msg.PublishTime.ToDateTime()}");
				Console.WriteLine($"Text: '{msg.Data.ToStringUtf8()}'");
			}
			// Acknowledge that we've received the messages. If we don't do this within 60 seconds (as specified
			// when we created the subscription) we'll receive the messages again when we next pull.
			subscriberApi.Acknowledge(subscriptionName, response.ReceivedMessages.Select(m => m.AckId));

			// Tidy up by deleting the subscription and the topic.
			subscriberApi.DeleteSubscription(subscriptionName);
			publisherApi.DeleteTopic(pubsubTopicName);
		}
	}
}
