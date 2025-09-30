import boto3
import json
import logging
from custom_encoder import CustomerEncoder
import os
from pymongo import MongoClient
client = MongoClient(host=os.environ.get("ATLAS_URI"))
mongo_db = client['reviewstable']  # Replace with your MongoDB database name
mongo_collection = mongo_db['reviewstable']  # Replace with your MongoDB reviews collection name


logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodbTableName = 'book-inventory'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(dynamodbTableName)

getMethod = 'GET'
postMethod = 'POST'
patchMethod = 'PATCH'
deleteMethod = 'DELETE'
healthPath = '/health'
bookPath = '/book'
booksPath = '/books'

def lambda_handler(event,context):
    logger.info(event)
    httpMethod = event['httpMethod']
    path = event['path']
    if httpMethod == getMethod and path == healthPath:
        response = buildResponse(200)
    elif httpMethod == getMethod and path == bookPath:
        response = getBook(event['queryStringParameters']['bookid'].replace('\n', ''))
    elif httpMethod == getMethod and path == booksPath:
        response = getBooks()
    elif httpMethod == postMethod and path == bookPath:
        response = saveBook(json.loads(event['body']))
    elif httpMethod == patchMethod and path == bookPath:
        requestBody = json.loads(event['body'])
        response = modifyBook(requestBody['bookid'],requestBody['updateKey'],requestBody['updateValue'])
    elif httpMethod == deleteMethod and path == bookPath:
        requestBody = json.loads(event['body'])
        response = deleteBook(requestBody['bookid'])
        #else:
        #    response = buildResponse(400, "Bad Request: 'bookid' missing in 'body'.")
    else:
        response = buildResponse(404, "Not Found")
    return response
    
def getBook(bookId):
    try:
        dynamo_response = table.get_item(
            Key={
                'bookid': bookid
            }
        )

        if 'Item' in dynamo_response:
            book_details = dynamo_response['Item']

            # Retrieve reviews for the book
            reviews = getReviewsForBook(bookid)

            # Combine reviews with book details
            book_details["reviews"] = reviews

            return buildResponse(200, book_details)
        else:
            return buildResponse(404, {"Message": f"bookid: {bookid} not found"})
    except Exception as e:
        logger.exception(f'Error retrieving book details and reviews: {str(e)}')
        return buildResponse(500, {"Message": "Error retrieving book details and reviews."})


def getReviewsForBook(bookId):
    try:
        # Fetch reviews for the book from your data source (e.g., MongoDB)
        # Modify this part based on your data source and format
        reviews_cursor = mongo_collection.find({"bookid": book_id})
        reviews = [{"Review Id": str(review["_id"]), "Comment": review["Comment"],"Reviewer": review["Reviewer"]} for review in reviews_cursor]

        return reviews
    except Exception as e:
        logger.exception(f'Error retrieving reviews for book {book_id}: {str(e)}')
        return []
        


        
def getBooks():
    try:
        response = table.scan()
        result = response['Items']
        
        while 'LastEvaluateKey' in response:
            response = table.scan(ExclusiveStartKey = response['LastEvaluatedKey'])
            result.extend(response['Items'])
            
        body = {
            "books": response
        }          
        return buildResponse(200, body)
    except:
        logger.exception('Custom Handling')
        
def saveBook(requestBody):
    try:
        table.put_item(Item=requestBody)
        body = {
            "Operation": "SAVE",
            "Message": "SUCCESS",
            "Item": requestBody
        }
        return buildResponse(200, body)
    except:
        logger.exception('Custom Handling')
        
def modifyBook(bookId,updateKey,updateValue):
    try:
        response = table.update_item(
            Key={
                'bookid': bookId
            },
            UpdateExpression= 'set %s = :value' % updateKey,
            ExpressionAttributeValues={
                ':value': updateValue
            },
            ReturnValues='UPDATED_NEW'
        )
        body= {
            "Opertaion": "UPDATE",
            "Message": "SUCCESS",
            "UpdatedAttributes": response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Custom Handling')
        
def deleteBook(bookId):
    try:
        response = table.delete_item(
            Key={
                'bookid': bookId
            },
            ReturnValues='ALL_OLD'
        )
        body={
            "Opertaion": "DELETE",
            "Message": "SUCCESS",
            "deleteItem": response
        }
        return buildResponse(200, body)
    except:
        logger.exception('Custom Handling')
        
def buildResponse(statusCode, body=None):
    response = {
        'statusCode': statusCode,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body'] = json.dumps(body, cls=CustomerEncoder)  # Use CustomerEncoder here
    return response
