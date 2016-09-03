<?php
namespace H69\ContentMapping\Elasticsearch;

use Elasticsearch\Client;
use H69\ContentMapping as CM;

/**
 * Class Adapter
 * Adapter for the elasticsearch Solr client
 *
 * @package H69\ContentMapping\Elasticsearch
 */
class Adapter implements CM\Adapter, CM\Adapter\ProgressListener
{
    /**
     * @var Client
     */
    private $elasticsearchClient;

    /**
     * @var string|array
     */
    private $index;

    /**
     * @var int Number of documents to collect before flushing intermediate results to Solr.
     */
    private $batchSize;

    /**
     * @var array[]
     */
    private $currentBatch = array();

    /**
     * @var array
     */
    private $messages = [];

    /**
     * Adapter constructor.
     *
     * @param Client       $elasticsearchClient
     * @param string|array $index
     * @param int          $batchSize
     */
    public function __construct($elasticsearchClient, $index, $batchSize = 20)
    {
        if (!$elasticsearchClient instanceof Client) {
            throw new \InvalidArgumentException('elasticsearch client have to be an instance of Elasticsearch\Client');
        }

        $this->elasticsearchClient = $elasticsearchClient;
        $this->index = $index;
        $this->batchSize = $batchSize;
    }

    /**
     * @return array
     */
    public function getMessages()
    {
        return $this->messages;
    }

    /**
     * Get an Iterator over all $type objects in the source/destination system, ordered by their ascending IDs.
     *
     * @param string $type       Type of Objects to return
     * @param string $indexQueue Whether all Objects or only new, updated or deleted Objects are returned for indexing
     *
     * @return \Iterator
     */
    public function getObjectsOrderedById($type)
    {
        $response = $this->elasticsearchClient->search([
            'index' => $this->index,
            'type'  => $type,
            'body'  => [
                'from'  => 0,
                'size'  => 1000000,
                'query' => [
                    'match_all' => [],
                ],
                'sort'  => [
                    '_id' => ['order' => 'asc'],
                ],
            ],
        ]);

        if (isset($response['hits']) && is_array($response['hits'])) {
            $this->messages[] = "ElasticsearchAdapter found " . $response['hits']['total'] ?: 0 . " objects for type " . $type;
            return new \ArrayIterator($response['hits']['hits'] ?: []);
        }
        $this->messages[] = "ElasticsearchAdapter found 0 objects for type " . $type;
        return new \ArrayIterator();
    }

    /**
     * Get the id of an object
     *
     * @param mixed $object
     *
     * @return int
     */
    public function idOf($object)
    {
        if (is_array($object) && array_key_exists('_id', $object)) {
            return $object['_id'];
        }
        return 0;
    }

    /**
     * Create a new object in the target system identified by ($id and $type).
     *
     * @param int    $id   ID of the newly created Object
     * @param string $type Type of the newly created Object
     *
     * @return mixed
     */
    public function createObject($id, $type)
    {
        $newDocument = [];
        $newDocument['_id'] = $id;
        $newDocument['_type'] = $type;
        $newDocument['_index'] = $this->index;
        $newDocument['_source'] = [];
        return $newDocument;
    }

    /**
     * Delete the $object from the target system.
     *
     * @param mixed $object
     */
    public function delete($object)
    {
        if (is_array($object) && array_key_exists('_id', $object) && array_key_exists('_type', $object)) {
            $this->currentBatch['body'][] = [
                'delete' => [
                    '_index' => $object['_index'] ?: $this->index,
                    '_type'  => $object['_type'],
                    '_id'    => $object['_id'],
                ],
            ];
        }
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that the $object has been updated.
     *
     * Although the name is somewhat misleading, it will be called after the Mapper has processed
     *   a) new objects created by the createObject() method
     *   b) changed objects created by the prepareUpdate() method *only if* the object actually changed.
     *
     * @param mixed $object
     */
    public function updated($object)
    {
        if (is_array($object) && array_key_exists('_id', $object) && array_key_exists('_type', $object)) {
            $this->currentBatch['body'][] = [
                'update' => [
                    '_index'             => $object['_index'] ?: $this->index,
                    '_type'              => $object['_type'],
                    '_id'                => $object['_id'],
                    'doc_as_upsert'      => true,
                    '_retry_on_conflict' => 3,
                ],
            ];

            $this->currentBatch['body'][] = [
                'doc' => ($object['_source'] ?: array()),
            ];
        }
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that all the in memory synchronization is
     * finished, i.e. can be persisted (e.g. by calling an entity manager's flush()).
     */
    public function commit()
    {
        if (count($this->currentBatch) === 0) {
            return;
        }

        $this->messages[] = "Flushing " . count($this->currentBatch) . " inserts, updates and deletes";

        $this->elasticsearchClient->bulk($this->currentBatch);
        $this->currentBatch = array();

        // to prevent memory exhaustion, we start a GC cycle collection run
        gc_collect_cycles();
    }

    /**
     * Callback method that will be called after every single object has been processed.
     *
     * @return void
     */
    public function afterObjectProcessed()
    {
        $batchCount = isset($this->currentBatch['body']) ? count($this->currentBatch['body']) : 0;
        if ($batchCount >= $this->batchSize) {
            $this->commit();
        }
    }
}
