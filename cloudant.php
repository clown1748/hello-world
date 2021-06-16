<?php
//Bearer Token Start
$token_httpheaders = array(
    'content-type: application/x-www-form-urlencoded',
    'accept: application/json'
);
$token_postfields = "grant_type=urn%3Aibm%3Aparams%3Aoauth%3Agrant-type%3Aapikey&apikey=MBz5toUiCuBlWhseu8JUi5EtLNOkMjxoAJ95dyWUtiqZ";

$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, 'https://iam.cloud.ibm.com/identity/token');
curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
curl_setopt($ch, CURLOPT_HTTPHEADER, $token_httpheaders);
curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
curl_setopt($ch, CURLOPT_POSTFIELDS, $token_postfields); 
$token_response = curl_exec($ch);
curl_close($ch);
$jfo = json_decode($token_response);
$token_bearer = $jfo->access_token;
//Bearer Token End

//variables
$cur_dbs_array = array();
$new_dbs_array = array();
$all_dbs_array = array();
$items = "";
$allitems = "";
$emptyitems = "";

$inputdata = file_get_contents("https://githubss.mybluemix.net/db-test-data.json");
$args = json_decode($inputdata,true);
		

//check if resutls[] returned is empty
if(empty($args['results'])) {
    $httpheader = array(
       'content-type: application/json',
       'Authorization: Bearer ' . $token_bearer
    );
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/_all_dbs');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'GET'); 
    $curl_response = curl_exec($ch);
    curl_close($ch);
    $empty_dbs_json = json_decode(trim($curl_response));
	if (!in_array('empty_results', $empty_dbs_json)) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/empty_results');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT'); 
        $curl_response = curl_exec($ch);
        curl_close($ch);
	}
  
	$emptyitems = '{"message": "zero results were returned in this result set",';
	$emptyitems = $emptyitems . '"added_to_cloudant_timestamp": "' . date("Y-m-d") . ' ' . date("h:i:sa") . '"}';
    $httpheader = array(
        'content-type: application/json',
        'Authorization: Bearer ' . $token_bearer
    );
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/empty_results');
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST'); 
    curl_setopt($ch, CURLOPT_POSTFIELDS, $emptyitems);
    $curl_response = curl_exec($ch);
    curl_close($ch);
} else {
  //Cloudant API List DB Names Start
  $httpheader = array(
     'content-type: application/json',
     'Authorization: Bearer ' . $token_bearer
  );
  $ch = curl_init();
  curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/_all_dbs');
  curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
  curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
  curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'GET'); 
  $curl_response = curl_exec($ch);
  curl_close($ch);
  $cur_dbs_json = json_decode(trim($curl_response));
  //Cloudant API List DB Names End
  
  //if no cloudant databases exisit they need to be created first, then documents can be inserted
  if (empty($cur_dbs_json)) {
  	//build array of new dbs from external data
  	foreach($args['results'] as $row) {
	  if (empty($row['type'])) {
	  	  $data_db = "no_type";
	  } else {
    	  $data_db = strtolower($row['type']);
	  }
	  //Q $data_db = strtolower($row['type']);
  	  if (!in_array($data_db, $new_dbs_array)) {
            array_push($new_dbs_array,$data_db);
        }
      }
  	//Create Cloudant DB
  	foreach($new_dbs_array as $db) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/' . strtolower($db));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT'); 
        $curl_response = curl_exec($ch);
        curl_close($ch);
      }
  	//Create Cloudant DB
  
  	//Insert Cloudant Docs
      $skip_array = array();
      foreach($new_dbs_array as $db) {
      	foreach($args['results'] as $row) {
          	foreach($row as $key => $val) {
				if (strtolower($row['type']) == $db && !in_array($db, $skip_array)) {
          			$items = $items . '"' . $key . '"' . ': "' . $val . '",';
      			} elseif (empty($row['type']) && !in_array($db, $skip_array)) {
    				$items = $items . '"' . $key . '"' . ': "' . $val . '",';
				}//if
      		}//inner foreach
              if (strtolower($row['type']) == $db && !in_array($db, $skip_array)) {
          		$items = $items . '"added_to_cloudant_timestamp":"' . date("Y-m-d") . ' ' . date("h:i:sa") . '",';
  				$items = '{' . substr($items,0,-1) . '},';
          		$allitems = $allitems . $items;
          		$items = "";
      		} elseif (empty($row['type']) && !in_array($db, $skip_array)) {
          		$items = $items . '"added_to_cloudant_timestamp":"' . date("Y-m-d") . ' ' . date("h:i:sa") . '",';
  				$items = '{' . substr($items,0,-1) . '},';
          		$allitems = $allitems . $items;
          		$items = "";
			}//if
      
          }//middle foreach
          array_push($skip_array,$db);
          $allitems = '{"docs": [' . substr($allitems,0,-1) . ']}';
      	$httpheader = array(
                 'content-type: application/json',
                 'Authorization: Bearer ' . $token_bearer
          );
      	$ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/' . $db . '/_bulk_docs');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST'); 
        curl_setopt($ch, CURLOPT_POSTFIELDS, $allitems);
        $curl_response = curl_exec($ch);
        curl_close($ch);
        $allitems = "";
      }//outer foreach
  	//Insert Cloudant Docs
  	
  //YES DBs
  } else {
  	//store current dbs in array
  	foreach ($cur_dbs_json as $db_name) {
  	  if (!in_array($db_name, $cur_dbs_array)) {
            array_push($cur_dbs_array,$db_name);
        }
      }
  	
    //store new dbs in array and create the new db
  	foreach($args['results'] as $row) {
	  if (empty($row['type'])) {
	  	  $data_db = "no_type";
	  } else {
    	  $data_db = strtolower($row['type']);
	  }      
	  //Q $data_db = strtolower($row['type']);
  	  if (!in_array($data_db, $new_dbs_array) AND !in_array($data_db, $cur_dbs_array)) {
            array_push($new_dbs_array,$data_db);
        }
      }
  	
  	//Create Cloudant DB
  	foreach($new_dbs_array as $db) {
        $ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/' . strtolower($db));
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'PUT'); 
        $curl_response = curl_exec($ch);
        curl_close($ch);
      }
  	//Create Cloudant DB
  	
  	//combine both new and old dbs
  	$all_dbs_array = array_merge($cur_dbs_array, $new_dbs_array);
  
  	//Insert Cloudant Docs
    $skip_array = array();
     foreach($all_dbs_array as $db) {
      foreach($args['results'] as $row) {
          	foreach($row as $key => $val) {
          		if (strtolower($row['type']) == $db && !in_array($db, $skip_array)) {
          			$items = $items . '"' . $key . '"' . ': "' . $val . '",';
      			} elseif (empty($row['type']) && !in_array($db, $skip_array)) {
    				$items = $items . '"' . $key . '"' . ': "' . $val . '",';
				}//if
      		}//inner foreach
            if (strtolower($row['type']) == $db && !in_array($db, $skip_array)) {
          		$items = $items . '"added_to_cloudant_timestamp":"' . date("Y-m-d") . ' ' . date("h:i:sa") . '",';
  				$items = '{' . substr($items,0,-1) . '},';
          		$allitems = $allitems . $items;
          		$items = "";
            } elseif (empty($row['type']) && !in_array($db, $skip_array)) {
          		$items = $items . '"added_to_cloudant_timestamp":"' . date("Y-m-d") . ' ' . date("h:i:sa") . '",';
  				$items = '{' . substr($items,0,-1) . '},';
          		$allitems = $allitems . $items;
          		$items = "";      		
			}//if
      
        }//middle foreach
        array_push($skip_array,$db);
        $allitems = '{"docs": [' . substr($allitems,0,-1) . ']}';
      	$httpheader = array(
                 'content-type: application/json',
                 'Authorization: Bearer ' . $token_bearer
          );
      	$ch = curl_init();
        curl_setopt($ch, CURLOPT_URL, 'https://e5637fdd-c04b-4af3-a5db-66bd1ccb6cf6-bluemix.cloudantnosqldb.appdomain.cloud/' . $db . '/_bulk_docs');
        curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($ch, CURLOPT_HTTPHEADER, $httpheader);
        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST'); 
        curl_setopt($ch, CURLOPT_POSTFIELDS, $allitems);
        $curl_response = curl_exec($ch);
        curl_close($ch);
        $allitems = "";
      }//outer foreach
  	//Insert Cloudant Docs
	
  }//if no cloudant databases exisit
}//if results is empty
?>
