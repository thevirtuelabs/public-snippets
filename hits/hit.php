<?php
error_reporting(0);
// We will be reading GET data
$start = microtime(1);
if (!$_GET) {
	echo 'No values found. Exiting Call';
	exit ();
}


// First fields: server side timestamp, hit class, event name, IP
date_default_timezone_set('America/Los_Angeles');
$responseData = array ();
$responseData['timestamp'] = date("Y-m-d H:i:s");
$responseData['hit_class'] = $_GET['hit_class'];
$responseData['event_name'] = $_GET['event_name'];
$responseData['ip'] = $_SERVER['REMOTE_ADDR'];



// Make sure that fields are in consistent order and hit class is known
switch ($_GET['hit_class']) {
	case 'ymhit': $fields = array( "uuid", "video_id", "embed_url", "embed_domain", "vendor_id", "vendor_name", "video_title", "duration", "category_id", "resolution", "instantArticle", "ymk_user", "noads", "mute", "autoplay", "embed_protocol", "embed_pathname", "placement", "root_domain"); break;
	default: echo 'ERROR: Unknown hit class "' . $_GET['hit_class'] .'"'; exit();
}

foreach ( $fields as $field ) {
	$responseData[$field] = $_GET[$field];
}

// Just print headers?
if($_GET['printheaders']) {
	echo '"' . implode('","', array_keys($responseData)) . '"'; 
	exit();
}

// Save data to CSV
$csv_path = './data/';
$filename = $_GET['hit_class'] . '__' . date('Y-m-d') . '.csv';
$file = new SplFileObject ( $csv_path . $filename, 'a' );
$file->fputcsv ( $responseData );
$file = null;


// Good bye
$end = microtime(1);
$total = ($end - $start);
$callback = (isset($_GET['callback']) ? $_GET['callback'] : null);
if (isset($callback))
    echo $callback . '(['.json_encode(['Hit saved in ' . round($total, 6) .'s.']).'])';
else
    echo '[JSON HERE]';
?>