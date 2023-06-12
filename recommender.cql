//Create Graph Projection 
CALL gds.graph.project(
  'recommenderTest11',
  ['User','Images','Restaurant','Cuisines'],
  {
    HAS_CUISINES: {
      orientation: 'UNDIRECTED',
      properties: 'weight'
    },
	SWIPED: {
      orientation: 'UNDIRECTED',
      properties: ['weight','swipeLeft']
    },
	 HAS_IMAGE: {
      orientation: 'UNDIRECTED',
      properties: 'weight'
    }
  }
)


//Filter the projection to only include likes (swipe right)
CALL gds.beta.graph.project.subgraph(
  'recommenderTest12', // name of the new projected graph
  'recommenderTest11', // name of the existing projected graph
  '*', // node predicate filter
  'r.swipeLeft = 0.0' // relationship predicate filter
)


//create embeddings
CALL gds.fastRP.mutate(
  'recommenderTest12',  // name of the projected graph
  {
    embeddingDimension: 256,
    mutateProperty: 'embedding',
    randomSeed: 42,
    relationshipWeightProperty: 'weight'
  }
)


//perform knn for the restaurant nodes based on embedding
CALL gds.knn.write(
  'recommenderTest12',  // name of the graph
  {
    nodeProperties: ['embedding'],
    writeRelationshipType: 'USERS_ALSO_LIKED',
    writeProperty: 'score',
    nodeLabels: ['Restaurant'],
    randomSeed: 42,
	concurrency: 1
  }
)
YIELD relationshipsWritten
RETURN relationshipsWritten


// query to get the recommended restaurant

MATCH (u:User {id: $userId})-[s:SWIPED {swipeLeft: 0}]->(i:Images)<-[e:HAS_IMAGE]-(r:Restaurant)
WITH r, s
ORDER BY s.timestamp DESC 
LIMIT 5
WITH collect(r) as recentRestaurants
MATCH (r)-[u:USERS_ALSO_LIKED]->(r1:Restaurant)
WHERE NOT r1 IN recentRestaurants
WITH r1,u.score as score, r1.latitude AS rLatitude, r1.longitude AS rLongitude, {latitude: $latitude, longitude: $longitude} AS location
WITH r1,score, rLatitude, rLongitude, location,
     distance(point({latitude: rLatitude, longitude: rLongitude}), point(location)) AS distance
MATCH (r1)-[oh:OPENING_HOURS]->(wd:Weekday)
WHERE toInteger(apoc.date.format(apoc.date.currentTimestamp(), 'HHmm')) >= oh.open_time 
  AND toInteger(apoc.date.format(apoc.date.currentTimestamp(), 'HHmm')) <= oh.close_time
  AND wd.weekday = apoc.date.format(apoc.date.currentTimestamp(), 'EEEE')
WITH r1.key as key, r1.restaurant_name as restaurant_name, score, distance
ORDER BY distance
LIMIT 3
RETURN key
ORDER BY score DESC
LIMIT 1


//query to get the image shown in the app

MATCH (r:Restaurant {key:$restaurantKey})-[:HAS_IMAGE]->(i:Images)<-[s:SWIPED]-(:User)
With i, count(s) as swipes
MATCH (i)<-[l:SWIPED {swipeLeft:0}]-(:User)
WITH  i, toFLoat(count(l))/toFloat(swipes) as ratio
order by  ratio desc
limit 5
WITH collect(i) as images
WITH images[toInteger(RAND() * size(images))] as selectedImage
RETURN selectedImage.id

