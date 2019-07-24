<?php

namespace CalosKao;

use DB;

class InsertOnDuplicateKey
{
    /**
     * Insert using mysql ON DUPLICATE KEY UPDATE.
     * @link http://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html
     *
     * Example:  $data = [
     *     ['id' => 1, 'name' => 'John'],
     *     ['id' => 2, 'name' => 'Mike'],
     * ];
     *
     * @param array $data is an array of array.
     * @param array $updateColumns NULL or [] means update all columns
     *
     * @return int 0 if row is not changed, 1 if row is inserted, 2 if row is updated
     */
    public static function insertOnDuplicateKey(string $table, array $data, array $updateColumns = null, string $connection = null)
    {
        if (empty($data)) {
            return false;
        }

        // Case where $data is not an array of arrays.
        if (!isset($data[0])) {
            $data = [$data];
        }

        // $sql = static::buildInsertOnDuplicateSql($data, $updateColumns);
        $sql = static::buildInsertOnDuplicateSql($table, $data, $updateColumns);

        $data = static::inLineArray($data);

        // return self::getModelConnectionName()->affectingStatement($sql, $data);
        return DB::connection($connection)->affectingStatement($sql, $data);
    }

    /**
     * Insert using mysql INSERT IGNORE INTO.
     *
     * @param string $table
     * @param array $data
     * @param string $connection
     *
     * @return int 0 if row is ignored, 1 if row is inserted
     */
    public static function insertIgnore(string $table, array $data, string $connection = null)
    {
        if (empty($data)) {
            return false;
        }

        // Case where $data is not an array of arrays.
        if (!isset($data[0])) {
            $data = [$data];
        }

        $sql = static::buildInsertIgnoreSql($table, $data);

        $data = static::inLineArray($data);

        return DB::connection($connection)->affectingStatement($sql, $data);
    }

    /**
     * Insert using mysql REPLACE INTO.
     *
     * @param string $table
     * @param array $data
     *
     * @return int 1 if row is inserted without replacements, greater than 1 if rows were replaced
     */
    public static function replace(string $table, array $data)
    {
        if (empty($data)) {
            return false;
        }

        // Case where $data is not an array of arrays.
        if (!isset($data[0])) {
            $data = [$data];
        }

        $sql = static::buildReplaceSql($table, $data);

        $data = static::inLineArray($data);

        return self::getModelConnectionName()->affectingStatement($sql, $data);
    }

    /**
     * Static function for getting the primary key.
     *
     * @return string
     */
    public static function getPrimaryKey()
    {
        $class = get_called_class();

        return (new $class())->getKeyName();
    }

    /**
     * Build the question mark placeholder.  Helper function for insertOnDuplicateKeyUpdate().
     * Helper function for insertOnDuplicateKeyUpdate().
     *
     * @param $data
     *
     * @return string
     */
    protected static function buildQuestionMarks($data)
    {
        $lines = [];
        foreach ($data as $row) {
            $count = count($row);
            $questions = [];
            for ($i = 0; $i < $count; ++$i) {
                $questions[] = '?';
            }
            $lines[] = '(' . implode(',', $questions) . ')';
        }

        return implode(', ', $lines);
    }

    /**
     * Get the first row of the $data array.
     *
     * @param array $data
     *
     * @return mixed
     */
    /**
     * Build the question mark placeholder.  Helper function for insertOnDuplicateKeyUpdate().
     * Helper function for insertOnDuplicateKeyUpdate().
     *
     * @see https://github.com/yadakhov/insert-on-duplicate-key/blob/dbca15aaa6dc39df77553837d4e8988d4c6245a7/src/InsertOnDuplicateKey.php#L143
     */
    protected static function getFirstRow(array $data)
    {
        if (empty($data)) {
            throw new \InvalidArgumentException('Empty data.');
        }

        reset($data);

        $first = current($data);

        if (!is_array($first)) {
            throw new \InvalidArgumentException('$data is not an array of array.');
        }

        return $first;
    }

    /**
     * Build a value list.
     *
     * @param array $first
     *
     * @return string
     */
    protected static function getColumnList(array $first)
    {
        if (empty($first)) {
            throw new \InvalidArgumentException('Empty array.');
        }

        return '`' . implode('`,`', array_keys($first)) . '`';
    }

    /**
     * Build a value list.
     *
     * @param array $updatedColumns
     *
     * @return string
     */
    protected static function buildValuesList(array $updatedColumns)
    {
        $out = [];

        foreach ($updatedColumns as $key => $value) {
            if (is_numeric($key)) {
                $out[] = sprintf('`%s` = VALUES(`%s`)', $value, $value);
            } else {
                $out[] = sprintf('%s = %s', $key, $value);
            }
        }

        return implode(', ', $out);
    }

    /**
     * Inline a multiple dimensions array.
     *
     * @param $data
     *
     * @return array
     */
    protected static function inLineArray(array $data)
    {
        return call_user_func_array('array_merge', array_map('array_values', $data));
    }

    /**
     * Build the INSERT ON DUPLICATE KEY sql statement.
     *
     * @param string $table
     * @param array $data
     * @param array $updateColumns
     *
     * @return string
     *
     * @see https://github.com/yadakhov/insert-on-duplicate-key/blob/dbca15aaa6dc39df77553837d4e8988d4c6245a7/src/InsertOnDuplicateKey.php#L234
     */
    protected static function buildInsertOnDuplicateSql(string $table, array $data, array $updateColumns = null)
    {
        $first = static::getFirstRow($data);
        $columnList = static::getColumnList($first);
        $sql  = "INSERT INTO `{$table}`($columnList) VALUES \n";
        $sql .=  static::buildQuestionMarks($data) . PHP_EOL;
        $sql .= 'ON DUPLICATE KEY UPDATE ';
        if (empty($updateColumns)) {
            $sql .= static::buildValuesList(array_keys($first));
        } else {
            $sql .= static::buildValuesList($updateColumns);
        }
        return $sql;
    }

    /**
     * Build the INSERT IGNORE sql statement.
     *
     * @param string $table
     * @param array $data
     *
     * @return string
     */
    protected static function buildInsertIgnoreSql(string $table, array $data)
    {
        $first = static::getFirstRow($data);
        $columnListString = static::getColumnList($first);
        $sql = "INSERT IGNORE INTO `{$table}`({$columnListString}) VALUES\n"
            . static::buildQuestionMarks($data);

        return $sql;
    }

    /**
     * Build REPLACE sql statement.
     *
     * @param array $data
     *
     * @return string
     */
    protected static function buildReplaceSql(string $table, array $data)
    {
        $first = static::getFirstRow($data);

        $sql  = "REPLACE INTO {$table}( {static::getColumnList($first)} ) VALUES \n";
        $sql .=  static::buildQuestionMarks($data);

        return $sql;
    }
}
