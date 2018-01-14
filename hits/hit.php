<?php

// We will be reading GET data
$start = microtime(1);
if (!$_GET) {
	echo 'No values found. Exiting Call';
	exit ();
}


// First fields: server side timestamp, hit class, event name
$responseData = array ();
$responseData['timestamp'] = date("Y-m-d H:i:s");


// Make sure that fields are in consistent order and hit class is known
switch ($_GET['hit_class']) {
	case 'ymhit': $fields = array( "uuid", "video_id", "embed_url", "embed_domain", "vendor_id", "vendor_name", "video_title", "duration", "category_id", "resolution", "instantArticle", "ymk_user", "noads", "mute", "autoplay", "embed_protocol", "embed_pathname", "placement", "root_domain"); break;
	default: echo 'ERROR: Unknown hit class "' . $_GET['hit_class'] .'"'; exit();
}

foreach ( $fields as $field ) {
	array_push ( $responseData, $_GET[$field] );
}

// Save data to CSV
$csv_path = './data/';
$filename = $_GET['hit_class'] . '__' . date('Y-m-d') . '__' . $_GET['event_name'] . '.csv';
$file = new SplFileObject ( $csv_path . $filename, 'a' );
$file->fputcsv ( $responseData );
$file = null;


// Good bye
$end = microtime(1);
$total = ($end - $start);
echo 'Writing process completed in ' . round($total, 6) .'s.';
?>