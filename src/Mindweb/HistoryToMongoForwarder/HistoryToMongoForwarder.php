<?php
namespace Mindweb\HistoryToMongoForwarder;

use MongoClient;
use Mindweb\Forwarder;

class HistoryToMongoForwarder implements Forwarder\Forwarder
{
    /**
     * @var \MongoCollection
     */
    private $collection;

    /**
     * @param array $configuration
     */
    public function __construct(array $configuration)
    {
        $username = !empty($configuration['username']) ? $configuration['username'] : false;
        $password = !empty($configuration['password']) ? $configuration['password'] : false;
        $host = !empty($configuration['host']) ? $configuration['host'] : 'localhost';
        $port = !empty($configuration['port']) ? $configuration['port'] : '27017';
        $fsync = !empty($configuration['fsync']) ? $configuration['fsync'] : false;
        $journal = !empty($configuration['journal']) ? $configuration['journal'] : false;

        $dsn = 'mongodb://';
        if ($username && $password) {
            $dsn .= $username . ':' . $password . '@';
        }
        $dsn .= $host . ':' . $port;

        $client = new MongoClient(
            $dsn,
            array(
                'connect' => true,
                'fsync' => $fsync,
                'journal' => $journal
            )
        );

        $db = !empty($configuration['db']) ? $configuration['db'] : 'db';
        $collection = !empty($configuration['collection']) ? $configuration['collection'] : 'raw_data';
        $this->collection = $client->selectCollection(
            $db,
            $collection
        );
    }

    /**
     * @param array $data
     * @return array
     */
    public function forward(array $data)
    {
        $this->collection->insert($data);
    }
} 