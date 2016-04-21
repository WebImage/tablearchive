<?php

function project_autoloader($str) {
	$path = __DIR__ . DIRECTORY_SEPARATOR . 'lib' . DIRECTORY_SEPARATOR . str_replace('\\', DIRECTORY_SEPARATOR, $str) . '.php';
	if (file_exists($path)) include_once($path);
}
spl_autoload_register('project_autoloader');