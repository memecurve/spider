from api.services.db import document as db_document
from api.services import http

def find_or_create(url):
    """
    Checks the database to see if a recent api.models.Document object was recently created. If it was do nothing. Otherwise download the URL and create a new api.models.Document object.

    :param str url: The url for the document

    :rtype: tuple( api.models.Document, list( str ) )
    :returns: A document representing the processed url and a list of errors, if any
    """
    db_document.find_one(type='sgml', )
