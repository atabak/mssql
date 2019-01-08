<?php

namespace Fuel\Core;

class Database_Mssql_Builder_Delete extends \Database_Query_Builder_Where
{

    protected $_table;

    public function __construct($table = null)
    {
        if ($table)
        {
            $this->_table = $table;
        }
        parent::__construct('', \DB::DELETE);
    }

    public function table($table)
    {
        $this->_table = $table;
        return $this;
    }

    public function compile($db = null)
    {
        if (!$db instanceof \Database_Connection)
        {
            $db = \Database_Connection::instance($db);
        }
        $query = 'FROM '.$db->quote_table($this->_table);
        if (!empty($this->_where))
        {
            $query .= ' WHERE '.$this->_compile_conditions($db, $this->_where);
        }
        if (!empty($this->_order_by))
        {
            $query .= ' '.$this->_compile_order_by($db, $this->_order_by);
        }
        if ($this->_limit !== null)
        {
            $query = 'DELETE TOP ('.$this->_limit.') '.$query;
        }
        else
        {
            $query = 'DELETE '.$query;
        }
        return $query;
    }

    public function reset()
    {
        $this->_table      = NULL;
        $this->_where      = array();
        $this->_order_by   = array();
        $this->_parameters = array();
        $this->_limit      = NULL;
        return $this;
    }

}
