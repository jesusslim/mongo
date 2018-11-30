<?php
/**
 * Created by PhpStorm.
 * User: jesusslim
 * Date: 2018/11/29
 * Time: 下午4:15
 */

namespace mongo\src\split;

/**
 * Class TableSplitByTime
 * @package mongo\src\split
 */
class TableSplitByTime implements TableSplitInterface
{

    const BY_MONTH = 1;
    const BY_YEAR = 2;

    protected $prefix;
    protected $by_what;

    /**
     * TableSplitByTime constructor.
     * @param string $prefix
     * @param int $by_what
     */
    public function __construct($prefix,$by_what)
    {
        $this->prefix = $prefix;
        $this->by_what = static::BY_MONTH;
    }

    /**
     * @param $split_value
     * @return bool|string
     */
    public function getTableSuffix($split_value)
    {
        switch ($this->by_what){
            case static::BY_MONTH:
                $fmt = 'Ym';
                break;
            case static::BY_YEAR:
                $fmt = 'Y';
                break;
            default:
                return false;
                break;
        }
        $suffix = date($fmt,$split_value);
        return $this->prefix.$suffix;
    }

    /**
     * @param $data
     * @param $key
     * @return bool|string
     */
    public function getTableSuffixByKey($data, $key)
    {
        if (!isset($data[$key])) return false;
        return $this->getTableSuffix($data[$key]);
    }
}