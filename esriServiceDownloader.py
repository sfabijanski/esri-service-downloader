import json
import requests
import sys

def download_service( url):
    """Takes a AGS service layer URL and downloads to json
    :param url: Url for the service layer
    """
    print 'About to download service layer:'
    
    print url

    # first get service info so we know how many ids
    # we can get on each pass
    service_info = get_service_info( url )
    if (not ( 'currentVersion' in service_info and
              'type' in service_info ) ):
        print 'Sorry. This does not appear to be an AGS service layer.'
        return

    print 'AGS version: ', service_info.get( 'currentVersion' )
    print 'Feature type: ', service_info.get( 'type' )
    
    # now get the objectids
    service_ids = get_service_ids( url )
    print 'Feature count: ', len( service_ids )
    
    
    # Get the number of records to request per pass
    chunk = get_chunk_size( service_info )
    print 'Grabbing in chunks of:', chunk


    # begin looping and requests
    print 'Beginning download'
    feature_service = get_service_features( url, service_ids, chunk )

    write_features_to_disk( feature_service, service_info.get('name') )
    print 'Done.'


def get_service_info( url ):
    """Gets the service layer info from the server
    """

    try:
        payload = { 'f': 'pjson' }
        r = requests.get( url, params=payload )
        return r.json()
    except:
        return { "error": "An error has occurred" } 
    #print r.text


def get_service_ids( url ):
    """Returns the list of objectids in the service layer
    """

    payload = { 'f': 'json', 'where': '1=1', 
        'returnIdsOnly': 'true',
        'text': ''}
    #print 'payload', payload
    url_query = ''.join( ( url, '/query') )
    r = requests.post(url_query, data=payload)
    ids = r.json().get('objectIds')
    return ids

def get_service_features( url, ids, chunk_size ):
    """Returns the entire featureset of the service layer as json"""
    start = 0
    end = len( ids )
    request_url = url + '/query'
    features = []
    result = None 

    for x in range( start, end, chunk_size ):
        ids_requested = ids[x: x + chunk_size ]
        #print x, ids_requested
        payload = { 'f': 'json', 'where': '1=1', 
            'objectIds': str( ids_requested )[1:-1], 'outSR': '4326',  
            'returnGeometry': 'true', 'outFields': '*', 
            'returnIdsOnly': 'false', 'returnDistinctValues': 'false', 
            'returnCountOnly': 'false'}
        result = requests.post( request_url, data=payload ) 
        #print result.json().get('features')
        features = features + result.json().get('features') 
        print '  Features completed: ', len(features)

    all_results = result.json()
    all_results['features']  = features 
    return all_results 


def write_features_to_disk( service, name ): 
    """Writes the features to disk.
    :param service: The features as a json file
    :param name: The name of the file (minus the extension)
    """

    file_name = './' + name.replace( ' ', '_' ) + '.json'  
    print 'Writing to:', file_name
    #print service
    with open( file_name, 'w') as file:
        json.dump( service, file )


def get_chunk_size( service_info ):
    """Returns the number of features to request
    """

    chunk = 500 # default
    try:
        chunk = service_info.get( 'maxRecordCount' )
    except:
        print "Unexpected error:", sys.exc_info()[0]
    
    return chunk

if __name__ == "__main__":
    if len( sys.argv ) != 2:
        print 'provide a mapservice url'
    else:
        url = sys.argv[1]
    download_service( url )

