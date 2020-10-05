
"""
CreateOrderFunction
"""

import datetime
import json
import os
import logging
from typing import List, Tuple
from urllib.parse import urlparse
import uuid
import boto3
import requests
## QLDB
import amazon.ion.simpleion as simpleion
from pyqldb.driver.qldb_driver import QldbDriver

LEDGER = os.environ["LEDGER"]
TABLE_NAME = os.environ["TABLE_NAME"]
CART_TABLE = os.environ["CART_TABLE"]
REGION = os.environ["REGION"]
REGION2 = os.environ["REGION2"]
APPSYNC_API_KEY = os.environ["APPSYNC_API_KEY"]
APPSYNC_API_ENDPOINT_URL = os.environ["APPSYNC_API_ENDPOINT_URL"]

headers = {
    'Content-Type': "application/graphql",
    'x-api-key': APPSYNC_API_KEY,
    'cache-control': "no-cache",
}

# Instantiating the driver
qldb_driver = QldbDriver(ledger_name=LEDGER)
dynamodb = boto3.resource("dynamodb") 
table = dynamodb.Table(TABLE_NAME) 
carttable = dynamodb.Table(CART_TABLE) 

crossregion = False
productQty = []

def validate_inventory(products: List[dict]) -> List[dict]:
    """
    Validate the inventory of the products in the order
    """

    for product in products:
        response = table.get_item(
            Key={
                'productid': product['productId']
            }
        )
        qty = response['Item']['QtyinStock']
        qtyinregion = response['Item']['QtyinRegion']
        name = response['Item']['Productname']
        price = response['Item']["Price"]

        if qty < product.get("quantity", 1):
            return (False, "Not enough inventory for {}".format(product['name']))
        else:
            newqty = qty - product.get("quantity", 1)

        if qtyinregion[REGION] < product.get("quantity", 1):
            crossregion = True
            newqtyinregion1 = 0
            newqtyinregion2 = qtyinregion[REGION2] - (product.get("quantity", 1) - qtyinregion[REGION])
            #store data for rollback
            oldprodqty1 = qtyinregion[REGION]
            oldprodqty2 = product.get("quantity", 1) - qtyinregion[REGION]
        else:
            newqtyinregion1 = qtyinregion[REGION] - product.get("quantity", 1)
            newqtyinregion2 = qtyinregion[REGION2]
            #store data for rollback
            oldprodqty1 = product.get("quantity", 1)
            oldprodqty2 = 0

        productQty.append({
            "productId": product["productId"],
            "name": name,
            "price": price,
            "prodquantity": product.get("quantity", 1),
            "quantity": newqty,
            "oldqtyinregion1": oldprodqty1,
            "oldqtyinregion2": oldprodqty2,
            "qtyinregion1": newqtyinregion1,
            "qtyinregion2": newqtyinregion2
        })
        product["name"] = name
        product["price"] = price
        product["quantity"]: product.get("quantity", 1)

    return (True, "There are enough product inventory to place the order")


def validate_order(products: List[dict]) -> List[dict]:
    """
    Validate the inventory of the products in the order
    """
    for product in products:
        response = table.get_item(
            Key={
                'productid': product['productId']
            }
        )
        qtyinregion1 = response['Item']['QtyinRegion'][REGION]
        # Create an http graphql request.
        query = "{getProduct(productid: %s) {QtyinRegion {%s}}}".format(product['productId'], REGION2)
        appresponse = session.request(
            url=APPSYNC_API_ENDPOINT_URL,
            method='POST',
            json={'query': query}
        )
        qtyinregion2 = appresponse["data"]["getProduct"]["QtyinRegion"][REGION2]
        
        if (qtyinregion1 + qtyinregion2) < product.get("quantity", 1):
            return (False, "Order cannot be accepted. Rolling back.")



        # response = table.get_item(
        #     Key={
        #         'productid': product['productId']
        #     }
        # )
        # qtyinregion1 = response['Item']['QtyinRegion'][REGION]
        # qtyinregion2 = response['Item']['QtyinRegion'][REGION2]

        # if qtyinregion2 < 0 or qtyinregion1 < 0:
        #     return (False, "Order cannot be accepted. Rolling back.")
    return (True, "This order is good to go")


def cleanup_products(products: List[dict]) -> List[dict]:
    """
    Cleanup products
    """

    return [{
        "productId": product["productId"],
        "quantity": product.get("quantity", 1)
    } for product in products]


def inject_order_fields(order: dict) -> dict:
    """
    Inject fields into the order and return the order
    """

    now = datetime.datetime.now()

    order["orderId"] = str(uuid.uuid4())
    order["status"] = "PENDING"
    order["createdDate"] = now.isoformat()
    order["modifiedDate"] = now.isoformat()
    order["total"] = sum([p["price"]*p.get("quantity", 1) for p in order["products"]])

    return order

 
def store_order(transaction_executor, arg_1):
    """
    Store the order in QLDB
    """
    transaction_executor.execute_statement("INSERT INTO Orders ?", arg_1)

def update_order(transaction_executor, order_id, status):
    """
    Update the order in QLDB
    """
    transaction_executor.execute_statement("UPDATE Orders SET status = ?  WHERE orderId = ?", status, order_id)

def update_inventory(products: List[dict]) -> List[dict]:
    """
    Update the inventory of the products in the order
    """
    for product in products:
        table.update_item(
            Key={
                'productid': product['productId']
            },
            UpdateExpression="SET QtyinStock = :val1, #qty.#reg = :val2, #qty.#reg2 = :val3",
            ExpressionAttributeNames={
                '#qty': 'QtyinRegion',
                '#reg': REGION,
                '#reg2': REGION2
            },
            ExpressionAttributeValues={
                ':val1': product['quantity'],
                ':val2': product['qtyinregion1'],
                ':val3': product['qtyinregion2']
            }
        )
        carttable.update_item(
            Key={
                'productid': product['productId']
            },
            UpdateExpression="SET Fulfilled = :val1",
            ExpressionAttributeValues={
                ':val1': True
            }
        )
    return (True, "Inventory updated")

def rollback_inventory(products: List[dict]) -> List[dict]:
    """
    Rooback the order
    """
    for product in products:
        table.update_item(
            Key={
                'productid': product['productId']
            },
            UpdateExpression="SET QtyinStock = QtyinStock + :val1, #qty.#reg = :qty.:reg + :val2, #qty.#reg2 = :qty.:reg2 + :val3",
            ExpressionAttributeNames={
                '#qty': 'QtyinRegion',
                '#reg': REGION,
                '#reg2': REGION2
            },
            ExpressionAttributeValues={
                ':qty': 'QtyinRegion',
                ':reg': REGION,
                ':reg2': REGION2,
                ':val1': product['prodquantity'],
                ':val2': product['oldqtyinregion1'],
                ':val3': product['oldqtyinregion2']
            }
        )
    return (True, "Inventory updated")


################### Start Handler  ################### 
######################################################

def lambda_handler(event, context):
    """
    Lambda function handler
    """

    # Basic checks on the event
    for key in ["order", "userId"]:
        if key not in event:
            return {
                "success": False,
                "message": "Invalid event",
                "errors": ["Missing {} in event".format(key)]
            }

    # Inject userId into the order
    order = event["order"]
    order["userId"] = event["userId"]

    # Cleanup & validate products
    validx = validate_inventory(order["products"])
    if validx is False:
        return {
            "success": False,
            "order": order,
            "message": "Order not completed. Not enough inventory."
        }

    # Inject fields in the order
    order = inject_order_fields(order)

    # create the Ion doc
    ion_order = simpleion.loads(simpleion.dumps(order))

    # Place order
    try:
        update_inventory(productQty)
        qldb_driver.execute_lambda(lambda x: store_order(x, ion_order))
    except:
        return {
            "success": False,
            "message": "Issue placing order"
        }

    ready = "READY FOR FULFILLMENT"
    cancel = "CANCELLED"

    #confirm order
    if crossregion == False:
        qldb_driver.execute_lambda(lambda x: update_order(x, order["orderId"], ready))
        order["status"] = ready
    else:
        orderstatus = validate_order(order["products"])
        if orderstatus:
            qldb_driver.execute_lambda(lambda x: update_order(x, order["orderId"], ready))
            order["status"] = ready
        else:
            rollback_inventory(productQty)
            qldb_driver.execute_lambda(lambda x: update_order(x, order["orderId"], cancel))
            order["status"] = cancel
            return {
                "success": False,
                "order": order,
                "message": "Order not completed. Please try again."
            }

    return {
        "success": True,
        "order": order,
        "message": "Order created"
    }