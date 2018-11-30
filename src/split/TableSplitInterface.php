<?php
/**
 * Created by PhpStorm.
 * User: jesusslim
 * Date: 2018/11/29
 * Time: 下午4:07
 */

namespace mongo\src\split;

/**
 * Interface TableSplitInterface
 * @package mongo\src\split
 */
interface TableSplitInterface
{

    /**
     * @return string|false
     */
    public function getTableSuffix($split_value);

    /**
     * @param $data
     * @param $key
     * @return string|false
     */
    public function getTableSuffixByKey($data,$key);
}