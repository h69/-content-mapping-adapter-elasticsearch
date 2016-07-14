<?php
namespace H69\ContentMapping\Elasticsearch\Tests;

use Elasticsearch\Client as ElasticsearchClient;
use Elasticsearch\Core\Client\Response as ElasticsearchResponse;
use Elasticsearch\QueryType\Select\Query\Query as ElasticsearchSelectQuery;
use Elasticsearch\QueryType\Select\Result\Result as ElasticsearchSelectResult;
use Elasticsearch\QueryType\Update\Query\Query as ElasticsearchUpdate;
use Elasticsearch\QueryType\Update\Query\Document\Document as ElasticsearchUpdateDocument;
use H69\ContentMapping\Elasticsearch\Adapter as ElasticsearchAdapter;

/**
 * Class ElasticsearchAdapterTest
 * @package H69\ContentMappinh\Elasticsearch\Tests
 */
class AdapterTest extends \PHPUnit_Framework_TestCase
{
    /**
     * client for elasticsearch
     *
     * @var ElasticsearchClient|\PHPUnit_Framework_MockObject_MockObject
     */
    private $solrClient;

    /**
     * adapter to test
     *
     * @var ElasticsearchAdapter
     */
    private $adapter;

    /**
     * Can be used as the parameter for $this->synchronizer->synchronize().
     *
     * @var string
     */
    private $type = 'arbitrary type';

    /**
     * @see \PHPUnit_Framework_TestCase::setUp()
     */
    protected function setUp()
    {
        $this->solrClient = $this->getMock(ElasticsearchClient::class);
        $this->adapter = new ElasticsearchAdapter($this->solrClient);
    }

    /**
     * @test
     */
    public function adapterConstructWithoutElasticsearchClass()
    {
        $this->setExpectedException(\InvalidArgumentException::class);
        $solrClient = null;
        new ElasticsearchAdapter($solrClient);
    }

    /**
     * @test
     */
    public function adapterReturnsOrderedObjects()
    {
        $query = new ElasticsearchSelectQuery();
        $response = new ElasticsearchResponse('', [0 => 'HTTP/1.1 200 OK']);

        $this->solrClient->expects($this->once())
            ->method('createSelect')
            ->will($this->returnValue($query));

        /* @var $result ElasticsearchSelectResult|\PHPUnit_Framework_MockObject_MockObject */
        $result = $this->getMock(ElasticsearchSelectResult::class, [], [$this->solrClient, $query, $response]);
        $this->solrClient->expects($this->once())
            ->method('execute')
            ->will($this->returnValue($result));

        $result->expects($this->once())
            ->method('getNumFound');

        $result->expects($this->once())
            ->method('getIterator');

        $this->adapter->getObjectsOrderedById($this->type);

        $message = $this->adapter->getMessages();
        $this->assertInternalType('array', $message);
        $this->assertNotEmpty($message);
    }

    /**
     * @test
     */
    public function adapterReturnsObjectId()
    {
        $obj = new \stdClass();
        $obj->id = 5;
        $this->assertEquals(5, $this->adapter->idOf($obj));
    }

    /**
     * @test
     */
    public function adapterReturnsElasticsearchDocumentOnCreateWithIdAndType()
    {
        $query = new ElasticsearchUpdate();
        $this->solrClient->expects($this->once())
            ->method('createUpdate')
            ->will($this->returnValue($query));

        $newObject = $this->adapter->createObject(5, $this->type);
        $this->assertInstanceOf(ElasticsearchUpdateDocument::class, $newObject);
        $this->assertEquals(5, $newObject->id);
        $this->assertEquals($this->type, $newObject->type);
    }

    /**
     * @test
     */
    public function adapterCommitReturnsOnEmptyQueue()
    {
        $this->assertNull($this->adapter->commit());
    }

    /**
     * @test
     */
    public function adapterCommitInsertUpdateDeletes()
    {
        $deleteObject = new \stdClass();
        $deleteObject->id = 5;
        $this->adapter->delete($deleteObject);

        $updateObject = new \stdClass();
        $updateObject->id = 5;
        $this->adapter->updated($updateObject);

        $query = $this->getMock(ElasticsearchUpdate::class);
        $this->solrClient->expects($this->once())
            ->method('createUpdate')
            ->will($this->returnValue($query));

        $query->expects($this->once())
            ->method('addDeleteByIds')
            ->with([$deleteObject->id]);

        $query->expects($this->once())
            ->method('addDocuments')
            ->with([$updateObject]);

        $query->expects($this->once())
            ->method('addCommit');

        $this->solrClient->expects($this->once())
            ->method('execute')
            ->with($query);

        $this->adapter->commit();

        $message = $this->adapter->getMessages();
        $this->assertInternalType('array', $message);
        $this->assertNotEmpty($message);
    }

    /**
     * @test
     */
    public function adapterPrepareUpdateReturnsElasticsearchDocument()
    {
        $object = new ElasticsearchUpdateDocument();
        $object->setField('id', 5);
        $object->setField('type', $this->type);

        /* @var $preparedObject ElasticsearchUpdateDocument */
        $preparedObject = $this->adapter->prepareUpdate($object);
        $this->assertInstanceOf(ElasticsearchUpdateDocument::class, $preparedObject);
        $this->assertEquals(5, $preparedObject->offsetGet('id'));
        $this->assertEquals($this->type, $preparedObject->offsetGet('type'));
    }

    /**
     * @test
     */
    public function adapterDoesNotCommitsWhenBatchSizeIsNotReached()
    {
        // create 5 deletions
        for ($i = 0; $i < 5; $i++) {
            $deleteObject = new \stdClass();
            $deleteObject->id = $i;
            $this->adapter->delete($deleteObject);
            unset($deleteObject);
        }

        // create 5 inserts/updates
        for ($i = 0; $i < 5; $i++) {
            $updateObject = new \stdClass();
            $updateObject->id = $i;
            $this->adapter->updated($updateObject);
            unset($updateObject);
        }

        $query = $this->getMock(ElasticsearchUpdate::class);
        $this->solrClient->expects($this->never())
            ->method('createUpdate');

        $query->expects($this->never())
            ->method('addDeleteByIds');

        $query->expects($this->never())
            ->method('addDocuments');

        $query->expects($this->never())
            ->method('addCommit');

        $this->solrClient->expects($this->never())
            ->method('execute');

        $this->adapter->afterObjectProcessed();
    }

    /**
     * @test
     */
    public function adapterCommitsWhenBatchSizeIsReached()
    {
        // create 10 deletions
        for ($i = 0; $i < 10; $i++) {
            $deleteObject = new \stdClass();
            $deleteObject->id = $i;
            $this->adapter->delete($deleteObject);
            unset($deleteObject);
        }

        // create 10 inserts/updates
        for ($i = 0; $i < 10; $i++) {
            $updateObject = new \stdClass();
            $updateObject->id = $i;
            $this->adapter->updated($updateObject);
            unset($updateObject);
        }

        $query = $this->getMock(ElasticsearchUpdate::class);
        $this->solrClient->expects($this->once())
            ->method('createUpdate')
            ->will($this->returnValue($query));

        $query->expects($this->once())
            ->method('addDeleteByIds');

        $query->expects($this->once())
            ->method('addDocuments');

        $query->expects($this->once())
            ->method('addCommit');

        $this->solrClient->expects($this->once())
            ->method('execute')
            ->with($query);

        $this->adapter->afterObjectProcessed();
    }
}
