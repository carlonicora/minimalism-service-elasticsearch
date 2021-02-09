<?php
namespace CarloNicora\Minimalism\Services\ElasticSearch;

use CarloNicora\Minimalism\Interfaces\ServiceInterface;
use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Exception;

class ElasticSearch implements ServiceInterface
{
    /** @var Client|null  */
    private ?Client $client=null;

    public function __construct(
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

        return $this->getClient()->exists($params);
    }

    /**
     * @param string $json
     * @return array
     * @throws Exception
     */
    public function bulk(
        string $json,
    ): array
    {
        $params = json_decode($json, true, 512, JSON_THROW_ON_ERROR);

        return $this->getClient()->bulk($params);
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

        return $this->getClient()->update($params);
    }

    /**
     * @param string $index
     * @param array $fields
     * @param string $term
     * @return array
     */
    public function search(
        string $index,
        array $fields,
        string $term,
    ): array
    {
        $params = [
            'index' => $index,
            'body' => [
                'query' => [
                    'multi_match' => [
                        'query' => $term,
                        'fields' => $fields
                    ]
                ]
            ]
        ];

        return $this->getClient()->search($params);
    }

    /**
     * @param string $index
     * @param array $fields
     * @param string $term
     * @return array
     */
    public function simpleSearch(
        string $index,
        array $fields,
        string $term,
    ): array
    {
        $searchResults = $this->search(
            index: $index,
            fields: $fields,
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