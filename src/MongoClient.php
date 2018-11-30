<?php
/**
 * Created by PhpStorm.
 * User: jesusslim
 * Date: 2018/11/29
 * Time: 下午4:01
 */

namespace mongo\src;


use mongo\src\split\TableSplitInterface;
use MongoDB\Client;
use Exception;

class MongoClient extends Client
{

    private $default_db;

    /**
     * @return string
     */
    public function getDefaultDb()
    {
        return $this->default_db;
    }

    /**
     * @param string $default_db
     */
    public function setDefaultDb($default_db)
    {
        $this->default_db = $default_db;
    }

    /**
     * @param string $table
     * @param array $data
     * @param string|null $db
     * @param TableSplitInterface|null $table_split
     * @param string $split_by_key
     * @return bool
     * @throws MongoClientException
     */
    public function insert($table,$data,$db = null,$table_split = null,$split_by_key = ''){
        if ($table_split && $split_by_key){
            $suffix = $table_split->getTableSuffixByKey($data,$split_by_key);
            if ($suffix === false){
                throw new MongoClientException('get split table fail');
            }else{
                $table = $table.$suffix;
            }
        }
        $col = $this->selectCollection($db ? : $this->default_db,$table);
        $r = $col->insertOne($data);
        if ($r->getInsertedCount() <= 0){
            throw new MongoClientException('Insert count 0');
        }
        return true;
    }

    /**
     * @param string $table
     * @param array $datas
     * @param string|null $db
     * @param TableSplitInterface|bool $table_split
     * @param string $split_by_key
     * @return bool
     * @throws MongoClientException
     */
    public function batchInsert($table,$datas,$db = null,$table_split = false,$split_by_key = ''){
        if ($table_split && $split_by_key){
            $all = [];
            foreach ($datas as $data){
                $suffix = $table_split->getTableSuffixByKey($data,$split_by_key);
                if ($suffix === false){
                    throw new MongoClientException('mongo batch insert exception,split table fail '.$table.' by '.$split_by_key);
                }else{
                    $all[$table.$suffix][] = $data;
                }
            }
        }else{
            $all[$table] = $datas;
        }
        foreach ($all as $tb => $dts){
            $col = $this->selectCollection($db ? : $this->default_db,$tb);
            $r = $col->insertMany($dts);
            if ($r->getInsertedCount() <= 0){
                throw new Exception('Insert count 0');
            }
        }
        return true;
    }

    /**
     * @param string $table
     * @param array $condition
     * @param array $update
     * @param string|null $db
     * @return bool
     */
    public function batchUpdate($table,$condition,$update,$db = null){
        $col = $this->selectCollection($db ? : $this->default_db,$table);
        $col->updateMany($condition,['$set' => $update]);
        return true;
    }

    /**
     * @param TableSplitInterface $table_split
     * @param $split_value
     * @param $table
     * @param array $filter
     * @param array $options
     * @return array
     * @throws MongoClientException
     */
    public function findWithTableSplit($table_split,$split_value,$table,$filter = [],$options = []){
        if ($table_split){
            $suffix = $table_split->getTableSuffix($split_value);
            if ($suffix === false){
                throw new MongoClientException('table split fail '.$split_value);
            }else{
                $table = $table.$suffix;
            }
        }
        $col = $this->selectCollection($this->default_db,$table);
        $r = $col->find($filter,$options);
        return $r->toArray();
    }
}