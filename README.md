# content-mapping-adapter-elasticsearch #

[![Build Status](https://travis-ci.org/h69/content-mapping-adapter-elasticsearch.svg?branch=master)](https://travis-ci.org/h69/content-mapping-adapter-elasticsearch)
[![Coverage Status](https://coveralls.io/repos/github/h69/content-mapping-adapter-elasticsearch/badge.svg?branch=master)](https://coveralls.io/github/h69/content-mapping-adapter-elasticsearch?branch=master)

Adapter for the elasticsearch-php client inside the [h69/content-mapping](https://github.com/h69/content-mapping) mini framework.


## Installation ##

    composer require h69/content-mapping-adapter-elasticsearch


## Usage ##

```php
use Elasticsearch\ClientBuilder;
use H69\ContentMapping\Synchronizer;
use H69\ContentMapping\Elasticsearch\Adapter as ElasticsearchAdapter;

$elasticsearchClient = ClientBuilder::create()->build();
$elasticsearchIndex = 'myIndex';

$sourceAdapter = ...;
$destinationAdapter = new ElasticsearchAdapter($elasticsearchClient, $elasticsearchIndex);
$typeToSynchronize = 'pages';

$synchronizer = new Synchronizer($sourceAdapter, $destinationAdapter);
$synchronizer->synchronize($typeToSynchronize, function($objectA, $objectB){
    ...
    //return Result::unchanged();
    return Result::changed($updatedObjectB);
});
```


## Credits, Copyright and License ##

This project/copy was started and developed by [h69](https://github.com/h69)
Copyright 2016. Code released under [the MIT license](LICENSE).
