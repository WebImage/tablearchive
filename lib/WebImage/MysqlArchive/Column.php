<?php 

namespace WebImage\MysqlArchive;

class Column {
	private $name;
	private $isAutoIncrement = false;
	public function __construct($name) {
		$this->name = $name;
	}
	public function getName() { return $this->name; }
	public function isAutoIncrement($true_false=null) {
		if (null === $true_false) return $this->isAutoIncrement;
		else if (is_bool($true_false)) $this->isAutoIncrement = $true_false;
		else throw new \Exception('Expecting boolean value');
		return $this->isAutoIncrement;
	}
}