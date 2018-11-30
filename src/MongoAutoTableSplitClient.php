<?php
/**
 * Created by PhpStorm.
 * User: jesusslim
 * Date: 2018/11/30
 * Time: 上午11:14
 */

namespace mongo\src;


use mongo\src\split\TableSplitInterface;

/**
 * Class MongoAutoTableSplitClient
 * @package mongo\src
 */
class MongoAutoTableSplitClient extends MongoClient
{

    /**
     * @var array [table_name => [TableSplitInterface,split_key_name]]
     */
    protected $rules;

    public function __construct($table_split_rules = [],$uri = 'mongodb://127.0.0.1/', array $uriOptions = [], array $driverOptions = [])
    {
        parent::__construct($uri, $uriOptions, $driverOptions);
        $this->rules = $table_split_rules;
    }

    /**
     * @param string $table
     * @param TableSplitInterface $table_split
     * @param string $split_key_name
     */
    public function rule($table,$table_split,$split_key_name){
        $this->rules[$table] = compact('table_split','split_key_name');
    }

    /**
     * @param string $table
     * @param array $data
     * @param string|null $split_key_name
     * @return bool
     */
    public function insertAuto($table,$data,$split_key_name = null){
        $rule = isset($this->rules[$table]) ? $this->rules[$table] : null;
        if ($rule){
            return $this->insert($table,$data,null,$rule['table_split'],$split_key_name ? $split_key_name : $rule['split_key_name']);
        }else{
            return $this->insert($table,$data);
        }
    }

    /**
     * @param string $table
     * @param array $datas
     * @param string|null $split_key_name
     * @return bool
     */
    public function batchInsertAuto($table,$datas,$split_key_name = null){
        $rule = isset($this->rules[$table]) ? $this->rules[$table] : null;
        if ($rule){
            return $this->batchInsert($table,$datas,null,$rule['table_split'],$split_key_name ? $split_key_name : $rule['split_key_name']);
        }else{
            return $this->batchInsert($table,$datas);
        }
    }
}