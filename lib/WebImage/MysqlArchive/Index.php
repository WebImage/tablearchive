<?php

namespace WebImage\MysqlArchive;

class Index {
	const NAME_PRIMARY = 'PRIMARY';
	
	private $tableName, $name, $nonUnique, $autoIncrement;
	private $columns = array();
	/**
	 * @param string $name The name of the index
	 * @param bool nonUnique whether this index is unique
	 * @param bool $autoIncrement Whether this index has an auto incrementing value
	 **/
	public function __construct($table_name, $name, $non_unique, $auto_increment) {
		$this->tableName = $table_name;
		$this->name = $name;
		$this->nonUnique = $non_unique;
		$this->autoIncrement = $auto_increment;
	}
	public function getTableName() { return $this->tableName; }
	public function getName() { return $this->name; }
	public function isNonUnique() { return $this->nonUnique; }
	public function isAutoIncrement() { return $this->autoIncrement; }
	public function addColumn(IndexColumn $column) { $this->columns[$column->getName()] = $column; }

	public function getColumns() { return array_values($this->columns); }
	public function getColumnNames() { return array_keys($this->columns); }
	
	public function isPrimary() {
		return ($this->getName() == self::NAME_PRIMARY);
	}
}