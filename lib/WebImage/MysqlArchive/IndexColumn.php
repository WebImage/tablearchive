<?php

namespace WebImage\MysqlArchive;

class IndexColumn {
	private $name;
	private $prefix; // used when index is on prefix of a column

	// Note that ascending/descending is not currently supported.
	public function __construct($input_name, $input_prefix) {
		$this->name = $input_name;
		$this->prefix = $input_prefix;
	}
	public function getName() { return $this->name; }
	public function getPrefix() { return $this->prefix; }
}