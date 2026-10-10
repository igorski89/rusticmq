package org.elasticmq.rest.sqs.integration.multisdk

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.services.sqs.{AmazonSQS, AmazonSQSClientBuilder}
import org.elasticmq.rest.sqs.integration.client.{AwsSdkV1SqsClient, AwsSdkV2SqsClient, SqsClient}
import org.scalatest.BeforeAndAfter
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.{SqsClient => AmazonSqsV2}

import java.net.URI
import scala.collection.JavaConverters._

// Run the original test traits without starting their embedded JVM server.
// Queue cleanup gives each original scenario a fresh RusticMQ queue namespace.
class RusticMqSdkV1TestSuite extends AmazonJavaMultiSdkTestSuite with BeforeAndAfter {
  override val awsAccountId = "123456789012"
  override val awsRegion = "elasticmq"
  private var currentClient: SqsClient = null
  override def testClient: SqsClient = currentClient
  private var sdk: AmazonSQS = null

  before {
    sdk = AmazonSQSClientBuilder.standard()
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials("x", "x")))
      .withEndpointConfiguration(new EndpointConfiguration("http://localhost:9321", "us-east-1"))
      .build()
    sdk.listQueues().getQueueUrls.asScala.foreach(url => sdk.deleteQueue(url))
    currentClient = new AwsSdkV1SqsClient(sdk)
  }
  after { if (sdk != null) sdk.shutdown() }
}

class RusticMqSdkV2TestSuite extends AmazonJavaMultiSdkTestSuite with BeforeAndAfter {
  override val awsAccountId = "123456789012"
  override val awsRegion = "elasticmq"
  private var currentClient: SqsClient = null
  override def testClient: SqsClient = currentClient
  private var sdk: AmazonSqsV2 = null

  before {
    sdk = AmazonSqsV2.builder()
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("x", "x")))
      .region(Region.EU_CENTRAL_1)
      .endpointOverride(new URI("http://localhost:9321"))
      .build()
    sdk.listQueues().queueUrls().asScala.foreach(url => sdk.deleteQueue(b => b.queueUrl(url)))
    currentClient = new AwsSdkV2SqsClient(sdk)
  }
  after { if (sdk != null) sdk.close() }
}
