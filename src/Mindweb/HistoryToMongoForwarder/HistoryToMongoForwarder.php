<?php
namespace Mindweb\HistoryToMongoForwarder;

use MongoClient;
use Mindweb\Forwarder;
use MongoDate;

class HistoryToMongoForwarder implements Forwarder\Forwarder
{
    /**
     * @var \MongoCollection
     */
    private $collection;

    /**
     * @var array
     */
    private $dateColumns;

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

        $this->dateColumns = !empty($configuration['dateColumns']) ? $configuration['dateColumns'] : array(
            'UTCTimestamp' => true
        );
    }

    /**
     * @param array $data
     * @return array
     */
    public function forward(array $data)
    {
        foreach ($this->dateColumns as $column => $isUTC) {
            $data[$column] = new MongoDate(strtotime($data[$column] . ($isUTC ? ' UTC' : '')));
        }

        $this->collection->insert($data);
    }
} 