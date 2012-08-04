# Interface Specification

## SELECT

### Inputs

```json
{
  "action": "select",
  "stmt": "SELECT * FROM xxx",
}
```
or
```json
{
  "action": "select",
  "stmt": "SELECT * FROM xxx WHERE a=? AND b=?",
  "params": [ { "a":10, "b":20 }, ... ]
}
```
or
```json
{
  "action": "select",
  "stmt": "SELECT * FROM xxx WHERE a=? AND b=?",
  "params": [ { "a":10, "b":20 }, ... ],
  "batchsize": 10
}
```

### Outputs

One of:

```json
{
  status: ok
  result: [ [ NAME:'a', AGE:32 ], ... ]
}
```
```json
{
  status: partial
  result: [ [ NAME:'a', AGE:32 ], ... ]
}
```
```json
{
  status: error
  exception: message
}
```

> If batchSize is less than available number of records, then partial result will be returned, and user will need to make another query to the replier within 10 seconds to get the next batch of data.  If 10 seconds pass with no query, the resultset and connection are closed automatically
