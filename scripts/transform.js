function transform(entityJson) {
  var entity = JSON.parse(entityJson);
  return JSON.stringify({
    "Some Property": "Some Key",
    "entity jsonified": entityJson
  });
}
