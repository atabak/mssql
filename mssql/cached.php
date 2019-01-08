<?php
namespace Fuel\Core;

class Database_Mssql_Cached extends \Database_Result implements \SeekableIterator, \ArrayAccess
{
	public function __construct($result, $sql, $as_object = null)
	{
		parent::__construct($result, $sql, $as_object);
		if (is_array($result))
		{
			$this->_results = $result;
		}
		elseif ($result instanceof \Mssql_Result)
		{
			if ($this->_as_object === false)
			{
				$this->_results = $this->_result->fetch_all(\PDO::FETCH_ASSOC);
			}
			elseif (is_string($this->_as_object))
			{
				$this->_results = array();
				while ($row = $this->_result->fetch_object(\PDO::FETCH_CLASS, $this->_as_object))
				{
					$this->_results[] = $row;
				}
			}
			else
			{
				$this->_results = array();
				while ($row = $this->_result->fetch_object())
				{
					$this->_results[] = $row;
				}
			}
		}
		else
		{
			throw new \FuelException('Database_Cached requires database results in either an array or a database object');
		}

		$this->_total_rows = count($this->_results);
	}
	public function __destruct()
	{
	}
	public function cached()
	{
		return $this;
	}
	public function seek($offset)
	{
		if ( ! $this->offsetExists($offset))
		{
			return false;
		}

		$this->_current_row = $offset;

		return true;
	}
	public function current()
	{
		if ($this->valid())
		{
			$this->_row = $this->_results[$this->_current_row];
			if ($this->_sanitization_enabled)
			{
				$this->_row = \Security::clean($this->_row, null, 'security.output_filter');
			}
		}
		else
		{
			$this->rewind();
		}

		return $this->_row;
	}
	public function next()
	{
		parent::next();

		isset($this->_results[$this->_current_row]) and $this->_row = $this->_results[$this->_current_row];
	}
	public function offsetExists($offset)
	{
		return isset($this->_results[$offset]);
	}
	public function offsetGet($offset)
	{
		if ( ! $this->offsetExists($offset))
		{
			return false;
		}
		else
		{
			$result = $this->_results[$offset];
		}
		if ($this->_sanitization_enabled)
		{
			$result = \Security::clean($result, null, 'security.output_filter');
		}

		return $result;
	}
	final public function offsetSet($offset, $value)
	{
		throw new \FuelException('Database results are read-only');
	}
	final public function offsetUnset($offset)
	{
		throw new \FuelException('Database results are read-only');
	}
}
