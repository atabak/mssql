<?php
namespace Fuel\Core;

class Database_Mssql_Builder_Select extends \Database_Query_Builder_Where
{
    protected $_select = array();
    protected $_distinct = false;
    protected $_from = array();
    protected $_join = array();
    protected $_group_by = array();
    protected $_having = array();
    protected $_offset = null;
    protected $_last_join;
    public function __construct(array $columns = null)
    {
        if (!empty($columns))
        {
            $this->_select = $columns;
        }
        parent::__construct('', \DB::SELECT);
    }
    public function distinct($value = true)
    {
        $this->_distinct = (bool) $value;

        return $this;
    }
    public function select($columns = null)
    {
        $columns = func_get_args();

        $this->_select = array_merge($this->_select, $columns);

        return $this;
    }
    public function select_array(array $columns, $reset = false)
    {
        $this->_select = $reset ? $columns : array_merge($this->_select, $columns);

        return $this;
    }
    public function from($tables)
    {
        $tables = func_get_args();

        $this->_from = array_merge($this->_from, $tables);

        return $this;
    }
    public function join($table, $type = NULL)
    {
        $this->_join[]    = $this->_last_join = new \Database_Query_Builder_Join($table, $type);
        return $this;
    }
    public function on($c1, $op, $c2)
    {
        $this->_last_join->on($c1, $op, $c2);
        return $this;
    }
    public function and_on($c1, $op, $c2)
    {
        $this->_last_join->and_on($c1, $op, $c2);
        return $this;
    }
    public function or_on($c1, $op, $c2)
    {
        $this->_last_join->or_on($c1, $op, $c2);
        return $this;
    }
    public function group_by($columns)
    {
        $columns = func_get_args();
        foreach ($columns as $idx => $column)
        {
            if (is_array($column))
            {
                foreach ($column as $c)
                {
                    $columns[] = $c;
                }
                unset($columns[$idx]);
            }
        }
        $this->_group_by = array_merge($this->_group_by, $columns);
        return $this;
    }
    public function having($column, $op = null, $value = null)
    {
        return call_fuel_func_array(array($this, 'and_having'), func_get_args());
    }
    public function and_having($column, $op = null, $value = null)
    {
        if ($column instanceof \Closure)
        {
            $this->and_having_open();
            $column($this);
            $this->and_having_close();
            return $this;
        }
        if (func_num_args() === 2)
        {
            $value = $op;
            $op    = '=';
        }
        $this->_having[] = array('AND' => array($column, $op, $value));
        return $this;
    }
    public function or_having($column, $op = null, $value = null)
    {
        if ($column instanceof \Closure)
        {
            $this->or_having_open();
            $column($this);
            $this->or_having_close();
            return $this;
        }
        if (func_num_args() === 2)
        {
            $value = $op;
            $op    = '=';
        }
        $this->_having[] = array('OR' => array($column, $op, $value));
        return $this;
    }
    public function having_open()
    {
        return $this->and_having_open();
    }
    public function and_having_open()
    {
        $this->_having[] = array('AND' => '(');
        return $this;
    }
    public function or_having_open()
    {
        $this->_having[] = array('OR' => '(');
        return $this;
    }
    public function having_close()
    {
        return $this->and_having_close();
    }
    public function and_having_close()
    {
        $this->_having[] = array('AND' => ')');
        return $this;
    }
    public function or_having_close()
    {
        $this->_having[] = array('OR' => ')');
        return $this;
    }
    public function offset($number)
    {
        $this->_offset = (int) $number;

        return $this;
    }
    public function compile($db = null)
    {
        if (!$db instanceof \Database_Connection)
        {
            $db = $this->_connection ? : \Database_Connection::instance($db);
        }
        $quote_ident = array($db, 'quote_identifier');
        $quote_table = array($db, 'quote_table');
        $query = 'SELECT ';
        if ($this->_distinct === TRUE)
        {
            $query .= 'DISTINCT ';
        }
        if (empty($this->_select))
        {
            $query .= '*';
        }
        else
        {
            $query .= implode(', ', array_unique(array_map($quote_ident, $this->_select)));
        }
        if (!empty($this->_from))
        {
            $query .= ' FROM '.implode(', ', array_unique(array_map($quote_table, $this->_from)));
        }
        if (!empty($this->_join))
        {
            $query .= ' '.$this->_compile_join($db, $this->_join);
        }
        if (!empty($this->_where))
        {
            $query .= ' WHERE '.$this->_compile_conditions($db, $this->_where);
        }
        if (!empty($this->_group_by))
        {
            $query .= ' GROUP BY '.implode(', ', array_map($quote_ident, $this->_group_by));
        }
        if (!empty($this->_having))
        {
            $query .= ' HAVING '.$this->_compile_conditions($db, $this->_having);
        }
        if (!empty($this->_order_by))
        {
            $query .= ' '.$this->_compile_order_by($db, $this->_order_by);
        }
        elseif (($this->_order_by !== NULL || $this->_limit !== NULL) && $this->_order_by == NULL)
        {
            $query .= ' ORDER BY (SELECT 0) ';
        }
        if ($this->_offset !== NULL || $this->_limit !== NULL)
        {
            if ($this->_offset == 0)
            {
                $this->_offset = '0';
            }
            if ($this->_order_by !== NULL && $this->_limit !== NULL)
            {
                $query .= ' OFFSET '.$this->_offset.' ROWS FETCH NEXT '.$this->_limit.' ROWS ONLY ';
            }
            elseif($this->_offset !== NULL && $this->_limit == NULL)
            {
                $query .= ' OFFSET '.$this->_offset.' ROWS FETCH NEXT 1 ROWS ONLY ';
            }
        }
        return $query;
    }
    public function reset()
    {
        $this->_select     = array();
        $this->_from       = array();
        $this->_join       = array();
        $this->_where      = array();
        $this->_group_by   = array();
        $this->_having     = array();
        $this->_order_by   = array();
        $this->_distinct   = false;
        $this->_limit      = null;
        $this->_offset     = null;
        $this->_last_join  = null;
        $this->_parameters = array();
        return $this;
    }
}
