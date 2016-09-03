<?php
namespace H69\ContentMapping\Elasticsearch\Tests;

use Elasticsearch\Client as ElasticsearchClient;
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
    private $elasticsearchClient;

    /**
     * adapter to test
     *
     * @var ElasticsearchAdapter
     */
    private $adapter;

    /**
     * Can be used to initialize elasticsearch adapter
     *
     * @var string
     */
    private $index = 'arbitrary index';

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
        $this->elasticsearchClient = $this->getMock(ElasticsearchClient::class, [], [], '', false);
        $this->adapter = new ElasticsearchAdapter($this->elasticsearchClient, $this->index);
    }

    /**
     * @test
     */
    public function adapterConstructWithoutElasticsearchClass()
    {
        $this->setExpectedException(\InvalidArgumentException::class);
        new ElasticsearchAdapter(null, $this->index);
    }

    /**
     * @test
     */
    public function adapterReturnsOrderedObjects()
    {
        $exampleSearchResult = array(
            'took'      => 1,
            'timed_out' => false,
            '_shards'   => array(
                'total'      => 5,
                'successful' => 5,
                'failed'     => 0,
            ),
            'hits'      => array(
                'total'     => 1,
                'max_score' => 0.30685282,
                'hits'      => array(
                    0 => array(
                        '_index'  => 'my_index',
                        '_type'   => 'my_type',
                        '_id'     => 'my_id',
                        '_score'  => 0.30685282,
                        '_source' => array(
                            'testField' => 'abc',
                        ),
                    ),
                ),
            ),
        );

        $this->elasticsearchClient->expects($this->once())
            ->method('search')
            ->willReturn($exampleSearchResult);

        $result = $this->adapter->getObjectsOrderedById($this->type);
        $this->assertInstanceOf(\ArrayIterator::class, $result);
        $this->assertEquals(1, count($result));

        $message = $this->adapter->getMessages();
        $this->assertInternalType('array', $message);
        $this->assertNotEmpty($message);
    }

    /**
     * @test
     */
    public function adapterReturnsEmptyObjects()
    {
        $exampleSearchResult = array();

        $this->elasticsearchClient->expects($this->once())
            ->method('search')
            ->willReturn($exampleSearchResult);

        $result = $this->adapter->getObjectsOrderedById($this->type);
        $this->assertInstanceOf(\ArrayIterator::class, $result);
        $this->assertEquals(0, count($result));

        $message = $this->adapter->getMessages();
        $this->assertInternalType('array', $message);
        $this->assertNotEmpty($message);
    }

    /**
     * @test
     */
    public function adapterReturnsObjectId()
    {
        $obj = array(
            '_id' => 5,
        );
        $this->assertEquals(5, $this->adapter->idOf($obj));
    }

    /**
     * @test
     */
    public function adapterReturnsElasticsearchDocumentArrayOnCreateWithIdAndType()
    {
        $newObject = $this->adapter->createObject(5, $this->type);
        $this->assertInternalType('array', $newObject);
        $this->assertEquals(5, $newObject['_id']);
        $this->assertEquals($this->type, $newObject['_type']);
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
        $expectedBulkParameters = array(
            'body' => [
                0 => [
                    'delete' => [
                        '_index' => $this->index,
                        '_type'  => $this->type,
                        '_id'    => 5,
                    ],
                ],
                1 => [
                    'update' => [
                        '_index'             => $this->index,
                        '_type'              => $this->type,
                        '_id'                => 5,
                        'doc_as_upsert'      => true,
                        '_retry_on_conflict' => 3,
                    ],
                ],
                2 => [
                    'doc' => array(),
                ],
            ],
        );
        $this->elasticsearchClient->expects($this->once())
            ->method('bulk')
            ->with($expectedBulkParameters);

        // add delete object
        $deleteObject = $this->adapter->createObject(5, $this->type);
        $this->adapter->delete($deleteObject);

        // add update object
        $updateObject = $this->adapter->createObject(5, $this->type);
        $this->adapter->updated($updateObject);

        // commit to create bulk
        $this->adapter->commit();

        $message = $this->adapter->getMessages();
        $this->assertInternalType('array', $message);
        $this->assertNotEmpty($message);
    }

    /**
     * @test
     */
    public function adapterDoesNotCommitsWhenBatchSizeIsNotReached()
    {
        // create 5 deletions
        for ($i = 0; $i < 5; $i++) {
            $deleteObject = $this->adapter->createObject(5, $this->type);
            $this->adapter->delete($deleteObject);
            unset($deleteObject);
        }

        // create 5 inserts/updates
        for ($i = 0; $i < 5; $i++) {
            $updateObject = $this->adapter->createObject(5, $this->type);
            $this->adapter->updated($updateObject);
            unset($updateObject);
        }

        $this->elasticsearchClient->expects($this->never())
            ->method('bulk');

        $this->adapter->afterObjectProcessed();
    }

    /**
     * @test
     */
    public function adapterCommitsWhenBatchSizeIsReached()
    {
        $expectedBulkParameters = array();

        // create 10 deletions
        for ($i = 1; $i <= 10; $i++) {
            $deleteObject = $this->adapter->createObject($i, $this->type);
            $this->adapter->delete($deleteObject);

            $expectedBulkParameters['body'][] = [
                'delete' => [
                    '_index' => $this->index,
                    '_type'  => $this->type,
                    '_id'    => $i,
                ],
            ];
        }

        // create 10 updates with empty doc
        for ($i = 1; $i <= 5; $i++) {
            $updateObject = $this->adapter->createObject($i, $this->type);
            $this->adapter->updated($updateObject);

            $expectedBulkParameters['body'][] = [
                'update' => [
                    '_index'             => $this->index,
                    '_type'              => $this->type,
                    '_id'                => $i,
                    'doc_as_upsert'      => true,
                    '_retry_on_conflict' => 3,
                ],
            ];
            $expectedBulkParameters['body'][] = [
                'doc' => array(),
            ];
        }

        $this->elasticsearchClient->expects($this->once())
            ->method('bulk')
            ->with($expectedBulkParameters);

        $this->adapter->afterObjectProcessed();
    }
}
