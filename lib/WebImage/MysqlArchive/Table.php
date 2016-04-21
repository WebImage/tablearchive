<?php

namespace WebImage\MysqlArchive;

class Table {
	private $table;
	private $columns = array();
	private $autoIncCol;
	private $indexes = array();
	
	public function getColumns() { return array_values($this->columns); }
	public function getColumnNames() { return array_keys($this->columns); }
	public function getIndexes() { return array_values($this->indexes); }
	
	public function addColumn(Column $column) {
		$this->columns[$column->getName()] = $column;
	}
	
	public function addIndex(Index $index) {
		$this->indexes[$index->getName()] = $index;
	}
	
}