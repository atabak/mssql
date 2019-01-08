<?php
namespace Fuel\Core;

class Database_Mssql_Result extends \Database_Result
{
	public function __construct($result, $sql, $as_object)
	{
		parent::__construct($result, $sql, $as_object);
		$this->_total_rows = $result->num_rows;
	}
	public function __destruct()
	{
		if ($this->_result instanceof \Mssql_Result)
		{
			$this->_result->free();
		}
	}
	public function cached()
	{
		return new \Database_MySQLi_Cached($this->_result, $this->_query, $this->_as_object);
	}
	public function next()
	{
		if ($this->_as_object === false)
		{
			$this->_row = $this->_result->fetch_array(\PDO::FETCH_ASSOC);
		}
		elseif (is_string($this->_as_object))
		{
			$this->_row = $this->_result->fetch_object(\PDO::FETCH_CLASS, $this->_as_object);
		}
		else
		{
			$this->_row = $this->_result->fetch_object();
		}
		if ($this->_sanitization_enabled)
		{
			$this->_row = \Security::clean($result, null, 'security.output_filter');
		}

		return $this->_row;
	}
}
