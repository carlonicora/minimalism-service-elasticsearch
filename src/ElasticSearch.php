<?php /** @noinspection PhpRedundantCatchClauseInspection */

namespace CarloNicora\Minimalism\Services\ElasticSearch;

use CarloNicora\Minimalism\Abstracts\AbstractService;
use CarloNicora\Minimalism\Interfaces\LoggerInterface;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\ElasticsearchException;

class ElasticSearch extends AbstractService
{
    /** @var Client|null  */
    private ?Client $client=null;

    public function __construct(
        private LoggerInterface $logger,
        private string $MINIMALISM_SERVICE_ELASTICSEARCH_HOST='localhost',
        private int $MINIMALISM_SERVICE_ELASTICSEARCH_PORT=9200,
        private string $MINIMALISM_SERVICE_ELASTICSEARCH_SCHEME='http',
        private ?string $MINIMALISM_SERVICE_ELASTICSEARCH_USER=null,
        private ?string $MINIMALISM_SERVICE_ELASTICSEARCH_PASS=null,
    )
    {
    }

    /**
     * @return Client
     */
    private function getClient(): Client
    {
        if ($this->client === null){
            $builder = ClientBuilder::create();

            if (str_starts_with($this->MINIMALISM_SERVICE_ELASTICSEARCH_SCHEME,'http')){
                $usernamePassword = '';
                if (
                    $this->MINIMALISM_SERVICE_ELASTICSEARCH_USER !== null
                    &&
                    $this->MINIMALISM_SERVICE_ELASTICSEARCH_PASS !== null
                ) {
                    $usernamePassword = $this->MINIMALISM_SERVICE_ELASTICSEARCH_USER
                        . ':'
                        . $this->MINIMALISM_SERVICE_ELASTICSEARCH_PASS
                        . '@';
                }
                $host = [
                    $this->MINIMALISM_SERVICE_ELASTICSEARCH_SCHEME
                    . '://'
                    . $usernamePassword
                    . $this->MINIMALISM_SERVICE_ELASTICSEARCH_HOST
                    . ':'
                    . $this->MINIMALISM_SERVICE_ELASTICSEARCH_PORT
                ];

                $builder->setHosts($host);
            }

            $this->client = $builder->build();
        }

        return $this->client;
    }

    /**
     * @param string $index
     * @param int $id
     * @return bool
     */
    public function exists(
        string $index,
        int $id,
    ): bool
    {
        $params = [
            'index' => $index,
            'id' => $id
        ];

        try {
            return $this->getClient()->exists($params);
        } catch (ElasticsearchException $exception) {
            $this->logger->error(
                message: 'Failed to process an exists request',
                domain: 'ElasticSearch',
                context: [
                    'params' => $params,
                    'exception' => [
                        'message' => $exception->getMessage(),
                        'file' => $exception->getFile(),
                        'line' => $exception->getLine(),
                        'trace' => $exception->getTraceAsString()
                    ]
                ]
            );
        }

        return false;
    }

    /**
     * @param array $params
     * @return array
     */
    public function bulk(
        array $params,
    ): array
    {
        try {
            return $this->getClient()->bulk($params);
        } catch (ElasticsearchException $exception) {
            $this->logger->error(
                message: 'Failed to process a bulk request',
                domain: 'ElasticSearch',
                context: [
                    'params' => $params,
                    'exception' => [
                        'message' => $exception->getMessage(),
                        'file' => $exception->getFile(),
                        'line' => $exception->getLine(),
                        'trace' => $exception->getTraceAsString()
                    ]
                ]
            );
        }

        return [];
    }

    /**
     * @param string $index
     * @param int $id
     * @param array $data
     * @return array
     */
    public function index(
        string $index,
        int $id,
        array $data
    ): array
    {
        $params = [
            'index' => $index,
            'id' => $id,
            'body' => $data
        ];

        if (!$this->exists($index, $id)){
            return $this->getClient()->index($params);
        }

        $params['body'] = [
            'doc' => $params['body']
        ];

        try {
            return $this->getClient()->update($params);
        } catch (ElasticsearchException $exception) {
            $this->logger->error(
                message: 'Failed to process an update request',
                domain: 'ElasticSearch',
                context: [
                    'params' => $params,
                    'exception' => [
                        'message' => $exception->getMessage(),
                        'file' => $exception->getFile(),
                        'line' => $exception->getLine(),
                        'trace' => $exception->getTraceAsString()
                    ]
                ]
            );
        }

        return [];
    }

    /**
     * @param string $index
     * @param array $fields
     * @param string $term
     * @param int $from
     * @param int $size
     * @return array
     */
    public function search(
        string $index,
        array $fields,
        string $term,
        int $from=0,
        int $size=25,
    ): array
    {
        $should = [];
        foreach ($fields as $field){
            $should [] = [
                'match' => [
                    $field => $term
                ]
            ];
        }

        foreach ($fields as $field){
            $should [] = [
                'match_phrase' => [
                    $field => $term
                ]
            ];
        }

        $should [] = [
            'query_string' => [
                'query' => $term .'*',
                'fields' => $fields
            ]
        ];

        $params = [
            'index' => $index,
            'body' => [
                'query' => [
                    'bool' => [
                        'should' => $should
                    ]
                ]
            ],
            'from' => $from,
            'size' => $size
        ];

        try {
            return $this->getClient()->search($params);
        } catch (ElasticsearchException $exception) {
            $this->logger->error(
                message: 'Failed to process a search request',
                domain: 'ElasticSearch',
                context: [
                    'params' => $params,
                    'exception' => [
                        'message' => $exception->getMessage(),
                        'file' => $exception->getFile(),
                        'line' => $exception->getLine(),
                        'trace' => $exception->getTraceAsString()
                    ]
                ]
            );
        }

        return [];
    }

    /**
     * @param string $index
     * @param array $fields
     * @param string $term
     * @param int $from
     * @param int $size
     * @return array
     */
    public function simpleSearch(
        string $index,
        array $fields,
        string $term,
        int $from=0,
        int $size=25,
    ): array
    {
        $searchResults = $this->search(
            index: $index,
            fields: $fields,
            term: $term,
            from: $from,
            size: $size,
        );

        $response = [];

        foreach ($searchResults['hits']['hits'] ?? [] as $result){
            $response[] = $result['_id'];
        }

        return $response;
    }

}