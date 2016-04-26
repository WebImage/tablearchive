<?php 

namespace WebImage\MysqlArchive;


class TableArchiver {
	
	const FLAG_NOLOG	= 0x00000001; // Do not log anything
	const FLAG_LOG_VERBOSE	= 0x00000002; // Logging in more detail
	const FLAG_LOG_STDOUT	= 0x00000004; // Send logged data to STDOUT
	const FLAG_DELETE_DATA	= 0x00000020; // Delete data from table after dumping
	protected $host, $port, $user, $pass, $dbName;
	
	/** @var resource Database **/
	protected $conn;
	/** @var string The user associated with mysql */
	protected $mysqlUser;
	/** @var string The directory to which outfiles are written */
	protected $outfileDir;
	protected $flags = 0;
	protected $sleep = null; // amount of time to sleep between requests
	/**
	 * log file pointers
	 **/
	protected $logFilePrefix, $logDir;
	/** @var resource File pointers to log files */
	protected $logFP, $errorFP;
	
	public function __construct($host, 
								$port, 
								$user,
								$pass,
								$db_name,
								$mysql_user='mysql',
								$outfile_dir='/tmp',
								$flags=0,
								$log_dir = '/var/tmp'
								) {
		$this->host = $host;
		$this->port = $port;
		$this->user = $user;
		$this->pass = $pass;
		$this->dbName = $db_name;
		
		$this->mysqlUser = $mysql_user;
		$this->flags = $flags;
		$this->logDir = rtrim($log_dir, '/') . '/';
		
		$this->initLogFiles();
		$this->initOutfileDir($outfile_dir);
		$this->initConnection();
	}
	
	protected function initLogFiles() {
		$this->logFilePrefix = $this->logDir . $this->dbName;
		
		$this->logFP = fopen($this->logFilePrefix . '.log', 'a+');
		$this->errorFP = fopen($this->logFilePrefix . '.error', 'a+');
		
		fwrite($this->logFP, $this->getLogHeader());
	}
	
	protected function getLogHeader() {
		$logtext = "Archive info: time=%s, db=%s, flags=%x\n";
		$logtext = sprintf($logtext, date('c'), $this->dbName, $this->flags);
		return $logtext;
	}
	
	protected function initOutfileDir($outfile_dir) {
		$outfile_dir = rtrim($outfile_dir, '/') . '/';
		
		if (!file_exists($outfile_dir)) throw new \Exception(sprintf('Missing outdir %s', $outfile_dir));
		
		$outfile_dir .= $this->dbName . '/';
		if (!file_exists($outfile_dir)) {
			$cmd = "sudo -u $this->mysqlUser mkdir $outfile_dir";
		    $this->executeShellCmd("Mkdir $this->dbName", $cmd);

		    if (!chmod($outfile_dir, 0700)) {
		      $this->raiseNonSqlException("chmod of $outfile_dir to 0700 failed");
		    }
		}
		
		$this->outfileDir = $outfile_dir;
		
	}
	
	public function initConnection() {
		$host_and_port = $this->host . (empty($this->port) ? '' : ':'.$this->port);

		$this->conn = mysql_connect($host_and_port, $this->user, $this->pass);
		mysql_select_db($this->dbName, $this->conn);
	}
	
	protected function ensureConnection() {
		if (!$this->conn || !mysql_ping($this->conn)) {
	      $this->initConnection();
	    }
	}
	public function archive($table_name, $where, $outfile_basename=null, $batch_size=500000) {
		
		$this->ensureConnection();
		
		$table = $this->getTable($table_name);
		
		if (null === $outfile_basename) $outfile_basename = $table_name . date('Ymd');
		
		$outfile_prefix = $this->outfileDir . $outfile_basename;
		
		$where = empty($where) ? '' : sprintf('WHERE %s', $where);
		
		$i = 0;
				
		#file_put_contents(sprintf('%s.%d', $outfile_prefix, $i), implode(',', $table->getColumnNames()));
		$columns = implode(',', $table->getColumnNames());
		$sql = 'SELECT "%s" INTO OUTFILE \'%s.%d\'';
		$sql = sprintf($sql, $columns, $outfile_prefix, $i);
		
		$this->executeSqlNoResult('Exporting columns', $sql);
		
		do {
			if ($i > 0 && null !== $this->sleep) sleep($this->sleep);

			$i ++;
			$start_ix = $i * $batch_size - $batch_size;
			// $end_ix = $start_ix + $batch_size - 1;
			
			$selectinto = "SELECT %s ".
	                      /* "FROM %s FORCE INDEX (PRIMARY) %s ". */
	                      "FROM %s %s ".
	                      "LIMIT %d, %d " .
	                      "INTO OUTFILE '%s.%d'";
			$columns = $table->getColumnNames();
			array_walk($columns, function($val, $key) use (&$columns) {
				$columns[$key] = '`' . $val . '`';
			});
			$columns = implode(', ', $columns);
			
			$selectinto = sprintf($selectinto, $columns,
				$this->quoted($table_name), $where,
				$start_ix, $batch_size,
				$outfile_prefix, $i);
				
			$this->executeSqlNoResult(sprintf('Selecting %d from %s limit %d:%d', $i, $table_name, $start_ix, $batch_size), $selectinto);
			
			$row_count = mysql_affected_rows($this->conn);
			
			$this->logVerbose(sprintf('Returned %d rows', $row_count));
			
		} while ($row_count >= $batch_size);
		
		if ($this->flags & self::FLAG_DELETE_DATA) {
			$sql = "DELETE FROM %s %s";
			$sql = sprintf($sql, $table_name, $where);
			$this->executeSqlNoResult(sprintf('Cleaning up table %s', $table_name), $sql);
		}
	}
	
	public function tableExists($table_name) {
		return (mysql_num_rows(mysql_query("SELECT * FROM information_schema.tables WHERE table_schema = '" . $this->dbName . "' AND table_name = '" . $table_name . "'")) > 0);
	}
	
	protected function logVerbose($txt) {
		if ($this->flags & self::FLAG_NOLOG) return;
		if ($this->flags & self::FLAG_LOG_VERBOSE) {
			$this->logCompact($txt);
		}
	}
	
	protected function logCompact($txt) {
		
		if ($this->flags & self::FLAG_NOLOG) return;
		
		$timestamp = date('Y-m-d G:i:s');
		
		$entry = $timestamp . ' ' . $txt . "\n";
		
		fwrite($this->logFP, $entry);
		
		if ($this->flags & self::FLAG_LOG_STDOUT) echo $entry;
		
	}
	
	protected function logError($txt) {
		
		fwrite($this->errorFP, $this->getLogHeader());
		fwrite($this->errorFP, "$txt\n");
		
		if ($this->flags & self::FLAG_LOG_STDOUT) echo "$txt\n";
		
	}
	
	protected function executeShellCmd($description, $cmd) {

		$cmd = escapeshellcmd($cmd);
		$output = array();
		$status = 0;
		exec($cmd, $output, $status);

		$this->logCompact("$description cmd=$cmd status = $status");
		foreach ($output as $outputline) {
			$this->logVerbose("$description cmd output line : $outputline");
		}

		if ($status !== 0) {
			throw new \Exception("$description cmd $cmd returned $status\n");
		}

	}
	protected function executeSqlNoResult($desc, $sql) {
		
		if ($this->flags & self::FLAG_LOG_VERBOSE) {
			$this->logVerbose($desc . "\n" . $sql . ";\n");
		} else {
			$this->logCompact($desc);
		}
		
		if (!mysql_query($sql, $this->conn)) {
			$error = mysql_error($this->conn);
	        $this->logError($sql . "\nError: " . $error);
			throw new \Exception('SQL error: ' . $sql);
		}
	}
	
	public function getTable($table_name) {
		
		$sql = "SELECT column_name, column_key, extra 
				FROM information_schema.columns
	            WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s' ";
		$sql = sprintf($sql, $table_name, $this->dbName);
		
		if (!($query = mysql_query($sql, $this->conn))) {
			throw new \Exception('Failed to get columns: ' . mysql_error($this->conn));
		}
		
		if (mysql_num_rows($query) == 0) throw new \Exception(sprintf('Table does not have any columns: %s', $table_name));
		
		$table = new Table($table_name);
		$auto_inc_col = null;
		while ($row = mysql_fetch_object($query)) {
			
			$auto_increment = false !== stripos($row->extra, 'auto_increment');
			
			$col = new Column($row->column_name);
			$col->isAutoIncrement($auto_increment);
			$table->addColumn($col);
			
			if ($col->isAutoIncrement()) $auto_inc_col = $col->getName(); // self::quotify($col->getName());
		}
		
		$sql = "SELECT * 
				FROM information_schema.statistics
	            WHERE TABLE_NAME = '%s' AND TABLE_SCHEMA = '%s' 
	            ORDER BY INDEX_NAME, SEQ_IN_INDEX";
		$sql = sprintf($sql, $table_name, $this->dbName);
		
		if (!($result = mysql_query($sql, $this->conn))) {
			new Exception('Failed to get index info ' . $sql);
	    }
		$prev_index_name = '';
	    $index = null;
	    $primary = null;
		
		while ($row = mysql_fetch_assoc($result)) {
			// $index_name = self::quotify($row['INDEX_NAME']);
			// $column_name = self::quotify($row['COLUMN_NAME']);
			$index_name = $row['INDEX_NAME'];
			$column_name = $row['COLUMN_NAME'];
			if ($prev_index_name != $index_name) {
				// is the 1st column of the index autoincrement colum?
				$auto = ($column_name === $auto_inc_col);
				
				$index = new Index($table_name, $index_name, $row['NON_UNIQUE']==1, $auto);
				if ($index->isPrimary()) $primary = $index;
				$table->addIndex($index);
			}
			
			$column = new IndexColumn($column_name, $row['SUB_PART']);
			$index->addColumn($column);
			$prev_index_name = $index_name;
		}
		
#		if (null === $primary) throw new \Exception(sprintf('%s table does not have a primary', $table_name));
		
		return $table;
	}
	
	protected function getColumnNames($table) {
		
		$columns = array();
		
		$sql = "SELECT column_name ".
	             "FROM information_schema.columns ".
	             "WHERE table_name ='%s' and table_schema='%s'";
				 
		$sql = sprintf($sql, $table, $this->dbName);
	}
	public function sleep($seconds) {
		$this->sleep = $seconds;
	}
	public static function quoted($str)
	{
		return '`'.$str.'`';
	}
}
