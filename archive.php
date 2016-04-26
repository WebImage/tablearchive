<?php

use WebImage\MysqlArchive\TableArchiver;
use WebImage\Cli\ArgumentParser;

require_once(__DIR__ . DIRECTORY_SEPARATOR . 'autoload.php');

$required_flags = array(
	'c' => 'The configuration file to use for db settings',
	'd' => 'Directory where archived data will be sent (must be mysql user writable)',
	'l' => 'Directory where log files are stored',
	't' => 'The database table to archive'
);
$optional_flags = array(
	'mysql-user' => 'The user account that runs the Mysql server',
	'where' => 'A SQL WHERE statement to filter results to be archived',
	'basename' => 'The basename of the file to be exported into the data directory',
	'v' => 'Verbose output',
	'delete' => 'Delete data from SQL table after dumping',
	'sleep' => 'Number of seconds to sleep between dump iterations (default=0)',
	'batchsize' => 'Number of records to process per batch'
);
$flags = array_merge($required_flags, $optional_flags);
$supported_flags = array_keys($flags);

try {
	$args = new ArgumentParser($argv, $supported_flags);
} catch (\InvalidArgumentException $e) {
	die($e->getMessage() . PHP_EOL . get_help($flags));
}
$config = null;
if ($args->isFlagSet('c')) {
	$config_file = $args->getFlag('c');
	$config = require($config_file);
	if (!is_array($config)) die('config file must return array' . PHP_EOL);
}

foreach(array_keys($required_flags) as $flag) {
	if (!$args->isFlagSet($flag)) die(sprintf('Missing required flag: %s', $flag) . PHP_EOL . get_help($flags));
}

$data_dir	= $args->getFlag('d'); // Directory where database dump goes
$log_dir	= $args->getFlag('l'); // Directory where output logs will be stored
$table		= $args->getFlag('t');
$mysql_user	= $args->getFlag('mysql-user', 'mysql');
$where		= $args->getFlag('where', '');
$opt_delete	= $args->isFlagSet('delete');
$opt_verbose	= $args->isFlagSet('v');
$opt_sleep	= $args->getFlag('sleep');
$opt_batchsize	= $args->getFlag('batchsize');

$host = get_param($config, 'host');
$port = get_param($config, 'port');
$user = get_param($config, 'user');
$pass = get_param($config, 'pass');
$database = get_param($config, 'database');

// selectTableIntoOutfile
$archive_flags = 0;
if ($opt_verbose) $archive_flags = $archive_flags | TableArchiver::FLAG_LOG_STDOUT;
if ($opt_delete) $archive_flags = $archive_flags | TableArchiver::FLAG_DELETE_DATA;

$archive = new TableArchiver(
	$host,
	$port,
	$user,
	$pass,
	$database,
	$mysql_user,
	$data_dir,
	$archive_flags,
	$log_dir
	);
if (null !== $opt_sleep) {
	if (!is_numeric($opt_sleep)) die('Invalid sleep value');
	$archive->sleep($opt_sleep);
}

$outfile_basename = $args->isFlagSet('basename') ? $args->getFlag('basename') : null;
$archive->archive($table, $where, $outfile_basename, $opt_batchsize);

function get_help(array $options) {
	$longest = 0;
	foreach($options as $key=>$val) {
		$len = strlen($key);
		if ($len > $longest) $longest = $len;
	}
	$return = '';
	foreach($options as $key=>$val) {
		$return .= str_pad($key, $longest+2) . $val . PHP_EOL;
	}
	return $return;
}

function get_param(array $params, $key, $default=null) {
	if (isset($params[$key])) return $params[$key];
	else return $default;
}
