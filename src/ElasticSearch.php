<?php
namespace CarloNicora\Minimalism\Services\ElasticSearch;

use CarloNicora\Minimalism\Interfaces\ServiceInterface;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;

class ElasticSearch implements ServiceInterface
{
    /** @var Client|null  */
    private ?Client $client=null;

    public function __construct(
        private $MINIMALISM_SERVICE_ELASTICSEARCH_HOST='localhost',
        private $MINIMALISM_SERVICE_ELASTICSEARCH_PORT=9200,
        private $MINIMALISM_SERVICE_ELASTICSEARCH_SCHEME='http',
        private $MINIMALISM_SERVICE_ELASTICSEARCH_USER=null,
        private $MINIMALISM_SERVICE_ELASTICSEARCH_PASS=null,
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
            'data' => $data
        ];

        return $this->getClient()->index($params);
    }

    /**
     * @param string $index
     * @param string $field
     * @param string $term
     * @return array
     */
    public function search(
        string $index,
        string $field,
        string $term,
    ): array
    {
        $params = [
            'index' => $index,
            'body' => [
                'query' => [
                    'match' => [
                        $field => $term
                    ]
                ]
            ]
        ];

        return $this->getClient()->search($params);
    }

    /**
     * @param string $index
     * @param string $field
     * @param string $term
     * @return array
     */
    public function simpleSearch(
        string $index,
        string $field,
        string $term,
    ): array
    {
        $searchResults = $this->search(
            index: $index,
            field: $field,
            term: $term,
        );

        $response = [];

        foreach ($searchResults['hits']['hits'] ?? [] as $result){
            $response[] = $result['_id'];
        }

        return $response;
    }

    /**
     *
     */
    public function initialise(): void
    {
    }

    /**
     *
     */
    public function destroy(): void
    {
        $this->client = null;
    }
}