#!/usr/bin/python
#
# Copyright 2009 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Samples for the Documents List API v3."""

__author__ = 'afshar@google.com (Ali Afshar)'

import os.path
import gdata.data
import gdata.acl.data
import gdata.docs.client
import gdata.docs.data
import gdata.sample_util


class SampleConfig(object):
  APP_NAME = 'GDataDocumentsListAPISample-v1.0'
  DEBUG = False


def CreateClient():
  """Create a Documents List Client."""
  client = gdata.docs.client.DocsClient(source=SampleConfig.APP_NAME)
  client.http_client.debug = SampleConfig.DEBUG
  # Authenticate the user with CLientLogin, OAuth, or AuthSub.
  try:
    gdata.sample_util.authorize_client(
        client,
        service=client.auth_service,
        source=client.source,
        scopes=client.auth_scopes
    )
  except gdata.client.BadAuthentication:
    exit('Invalid user credentials given.')
  except gdata.client.Error:
    exit('Login Error')
  return client


def PrintResource(resource):
  """Display a resource to Standard Out."""
  print resource.resource_id.text, resource.GetResourceType()


def PrintFeed(feed):
  """Display a feed to Standard Out."""
  for entry in feed.entry:
    PrintResource(entry)


def _GetDataFilePath(name):
  return os.path.join(
      os.path.dirname(
          os.path.dirname(
              os.path.dirname(__file__))),
      'tests', 'gdata_tests', 'docs', 'data', name)


def GetResourcesSample():
  """Get and display first page of resources."""
  client = CreateClient()
  # Get a feed and print it
  feed = client.GetResources()
  PrintFeed(feed)


def GetAllResourcesSample():
  """Get and display all resources, using pagination."""
  client = CreateClient()
  # Unlike client.GetResources, this returns a list of resources
  for resource in client.GetAllResources():
    PrintResource(resource)


def GetResourceSample():
  """Fetch 5 resources from a feed, then again individually."""
  client = CreateClient()
  for e1 in client.GetResources(limit=5).entry:
    e2 = client.GetResource(e1)
    print 'Refetched: ', e2.title.text, e2.resource_id.text


def GetMetadataSample():
  """Get and display the Metadata for the current user."""
  client = CreateClient()
  # Fetch the metadata entry and display bits of it
  metadata = client.GetMetadata()
  print 'Quota'
  print '  Total:', metadata.quota_bytes_total.text
  print '  Used:', metadata.quota_bytes_used.text
  print '  Trashed:', metadata.quota_bytes_used_in_trash.text
  print 'Import / Export'
  for input_format in metadata.import_formats:
    print '  Import:', input_format.source, 'to', input_format.target
  for export_format in metadata.export_formats:
    print '  Export:', export_format.source, 'to', export_format.target
  print 'Features'
  for feature in metadata.features:
    print '  Feature:', feature.name.text
  print 'Upload Sizes'
  for upload_size in metadata.max_upload_sizes:
    print '  Kind:', upload_size.kind, upload_size.text


def GetChangesSample():
  """Get and display the Changes for the user."""
  client = CreateClient()
  changes = client.GetChanges()
  for change in changes.entry:
    print change.title.text, change.changestamp.value


def GetResourceAclSample():
  """Get and display the ACL for a resource."""
  client = CreateClient()
  for resource in client.GetResources(limit=5).entry:
    acl_feed = client.GetResourceAcl(resource)
    for acl in acl_feed.entry:
      print acl.role.value, acl.scope.type, acl.scope.value


def CreateEmptyResourceSample():
  """Create an empty resource of type document."""
  client = CreateClient()
  document = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  document = client.CreateResource(document)
  print 'Created:', document.title.text, document.resource_id.text


def CreateCollectionSample():
  """Create an empty collection."""
  client = CreateClient()
  col = gdata.docs.data.Resource(type='folder', title='My Sample Folder')
  col = client.CreateResource(col)
  print 'Created collection:', col.title.text, col.resource_id.text


def CreateResourceInCollectionSample():
  """Create a collection, then create a document in it."""
  client = CreateClient()
  col = gdata.docs.data.Resource(type='folder', title='My Sample Folder')
  col = client.CreateResource(col)
  print 'Created collection:', col.title.text, col.resource_id.text
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  doc = client.CreateResource(doc, collection=col)
  print 'Created:', doc.title.text, doc.resource_id.text


def UploadResourceSample():
  """Upload a document, and convert to Google Docs."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  # Set the description
  doc.description = gdata.docs.data.Description(
      'This is a simple Word document.')
  # This is a convenient MS Word doc that we know exists
  path = _GetDataFilePath('test.0.doc')
  print 'Selected file at: %s' % path
  # Create a MediaSource, pointing to the file
  media = gdata.data.MediaSource()
  media.SetFileHandle(path, 'application/msword')
  # Pass the MediaSource when creating the new Resource
  doc = client.CreateResource(doc, media=media)
  print 'Created, and uploaded:', doc.title.text, doc.resource_id.text


def UploadUnconvertedFileSample():
  """Upload a document, unconverted."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Raw Doc')
  path = _GetDataFilePath('test.0.doc')
  media = gdata.data.MediaSource()
  media.SetFileHandle(path, 'application/msword')
  # Pass the convert=false parameter
  create_uri = gdata.docs.client.RESOURCE_UPLOAD_URI + '?convert=false'
  doc = client.CreateResource(doc, create_uri=create_uri, media=media)
  print 'Created, and uploaded:', doc.title.text, doc.resource_id.text


def DeleteResourceSample():
  """Delete a resource (after creating it)."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  doc = client.CreateResource(doc)
  # Delete the resource we just created.
  client.DeleteResource(doc)


def AddAclSample():
  """Create a resource and an ACL."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  doc = client.CreateResource(doc)
  acl_entry = gdata.docs.data.AclEntry(
      scope=gdata.acl.data.AclScope(value='user@example.com', type='user'),
      role=gdata.acl.data.AclRole(value='reader'),
  )
  client.AddAclEntry(doc, acl_entry, send_notifications=False)


def DeleteAclSample():
  """Create an ACL entry, and delete it."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  doc = client.CreateResource(doc)
  acl_entry = gdata.docs.data.AclEntry(
      scope=gdata.acl.data.AclScope(value='user@example.com', type='user'),
      role=gdata.acl.data.AclRole(value='reader'),
  )
  acl_entry = client.AddAclEntry(doc, acl_entry)
  client.DeleteAclEntry(acl_entry)


def AddAclBatchSample():
  """Add a list of ACLs as a batch."""
  client = CreateClient()
  doc = gdata.docs.data.Resource(type='document', title='My Sample Doc')
  doc = client.CreateResource(doc)
  acl1 = gdata.docs.data.AclEntry(
      scope=gdata.acl.data.AclScope(value='user1@example.com', type='user'),
      role=gdata.acl.data.AclRole(value='reader'),
      batch_operation=gdata.data.BatchOperation(type='insert'),
  )
  acl2 = gdata.docs.data.AclEntry(
      scope=gdata.acl.data.AclScope(value='user2@example.com', type='user'),
      role=gdata.acl.data.AclRole(value='reader'),
      batch_operation=gdata.data.BatchOperation(type='insert'),
  )
  # Create a list of operations to perform together.
  acl_operations = [acl1, acl2]
  # Perform the operations.
  client.BatchProcessAclEntries(doc, acl_operations)


def GetRevisionsSample():
  """Get the revision history for resources."""
  client = CreateClient()
  for entry in client.GetResources(limit=55).entry:
    revisions = client.GetRevisions(entry)
    for revision in revisions.entry:
      print revision.publish, revision.GetPublishLink()


if __name__ == '__main__':
  import samplerunner
  samplerunner.Run(__file__)
