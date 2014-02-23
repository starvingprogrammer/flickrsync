import flickr_api
import argparse
import datetime
import hashlib
import sys
import os
import ConfigParser
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, create_engine, and_
from sqlalchemy.orm import backref, relationship, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect

# https://github.com/alexis-mignon/python-flickr-api/wiki/Tutorial
# http://www.flickr.com/groups/api/discuss/72157594497877875/
# https://www.bionicspirit.com/blog/2011/10/29/how-i-use-flickr.html

Base = declarative_base()

class Photo(Base):
    __tablename__ = 'photos'
    id = Column(Integer, primary_key=True)
    title = Column(String)
    tags = Column(String)
    date_taken = Column(DateTime)
    md5 = Column(String)

class PhotoPhotosetLink(Base):
    __tablename__ = 'photo_photoset'
    photo_id = Column(Integer, ForeignKey('photos.id'), primary_key=True)
    photoset_id = Column(Integer, ForeignKey('photosets.id'), primary_key=True)

class Photoset(Base):
    __tablename__ = 'photosets'
    id = Column(Integer, primary_key=True)
    title = Column(String)

engine = create_engine('sqlite:///%s' % os.path.expanduser('~/.flickr_sync.db'))
Base.metadata.create_all(engine)
session = Session(engine)


config_filename = os.path.expanduser('~/.flickr_sync.cfg')
config = ConfigParser.ConfigParser(allow_no_value=True)
config.read(config_filename)
API_KEY = config.get('flickr', 'API_KEY')
SHARED_SECRET = config.get('flickr', 'SHARED_SECRET')
USER_ID = config.get('flickr', 'USER_ID')

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

parser_update = subparsers.add_parser('update')
parser_update.add_argument('md5', action='store_true')

parser_list = subparsers.add_parser('list')
parser_list.add_argument("--photos", action='store_true')

parser_dbtest = subparsers.add_parser('dbtest')

args = parser.parse_args()

flickr_api.set_keys(api_key = API_KEY, api_secret = SHARED_SECRET)
"""
a = flickr_api.auth.AuthHandler()
perms = "write"
url = a.get_authorization_url(perms)
print url
token = raw_input("enter token: ")

a.set_verifier(token)
flickr_api.set_auth_handler(a)
a.save('flickr_api.auth')
"""
flickr_api.set_auth_handler(os.path.expanduser('~/.flickr_api.auth'))
user = flickr_api.test.login()
if args.command == 'list':
    pages = user.getPhotos().info.pages
    for i in range(1,pages):
        for p in user.getPhotos(page=i,extras='machine_tags,date_taken'):
            print i, p.title, p.datetaken, p, 'machine_tags:', 'checksum:md5=' in p.machine_tags
    sys.exit(0)
if args.command == 'dbtest':
    photosets = user.getPhotosets()
    photocnt = 0
    for ps in photosets:
        if session.query(Photoset).filter(Photoset.id==ps.id).count() == 0:
            photoset = Photoset(id=ps.id, title=ps.title)
            session.add(photoset)
            session.commit()
        
        for p in ps.getPhotos(extras='machine_tags,date_taken,date_upload,tags'):
            photocnt += 1
            print photocnt, p.title, p.datetaken, p, 'machine_tags:', 'checksum:md5=' in p.machine_tags
            md5 = ''
            if 'checksum:md5=' in p.machine_tags:
                md5 = [mt for mt in p.machine_tags.split(' ') if mt.startswith('checksum:md5=')][0].split('=')[1]
            date_taken = datetime.datetime.strptime(p.datetaken, '%Y-%m-%d %H:%M:%S')
            if (session.query(Photo).filter(Photo.id==p.id).count() == 0):
                photo = Photo(id=p.id, md5=md5, title=p.title, tags=p.tags, date_taken=date_taken)
                session.add(photo)
            if (session.query(PhotoPhotosetLink).filter(and_(PhotoPhotosetLink.photo_id==p.id, 
                                                             PhotoPhotosetLink.photoset_id==ps.id)
                                                       ).count() == 0):
                ppl = PhotoPhotosetLink(photo_id=p.id, photoset_id=ps.id)
                session.add(ppl)
                session.commit()
        
    sys.exit(0)
photos_total = user.getPhotos().info.total
photosets = user.getPhotosets()
photos_count = 0
start_time = datetime.datetime.now()
basedir = '/data/flickr_backup_20140222'
if not os.path.isdir(basedir):
    os.mkdir(basedir)
for ps in photosets:
  ps_dir = '%s/%s' % (basedir, ps.title.replace('/', '_'))
  if not os.path.isdir(ps_dir):
      os.mkdir(ps_dir)
  print ps_dir
  ps_date_create = str(datetime.datetime.fromtimestamp(float(ps.date_create)))
  os.utime(ps_dir, (int(ps.date_create), int(ps.date_create)))
  date_update = str(datetime.datetime.fromtimestamp(float(ps.date_update)))
  print "%s %s: photos=%s, videos=%s, date_update=%s" % (ps_date_create, ps.title, ps.photos, ps.videos, date_update)

  # get meta via extras so another API call can be avoided (via getInfo or using property that will 
  # transparently make another call
  for p in ps.getPhotos(extras='machine_tags,date_taken,date_upload,tags'):
      print
      #info = p.getInfo()
      date_uploaded = str(datetime.datetime.fromtimestamp(float(p.dateupload)))
      tags = p.tags
      print ps.title, "\t%s %s: tags=%s; taken=%s" % (date_uploaded, p.title, tags, p.datetaken)
      photos_count += 1
      duration = datetime.datetime.now() - start_time
      if duration.seconds < 1:
          duration_estimate_minutes = 0
      else:
          duration_estimate_minutes = round((photos_total-photos_count) * (duration.seconds*1.0/photos_count) / 60)

      print '%d/%d: %s percent  %s seconds; %s minute estimate' % (photos_count, photos_total, round(photos_count*100.0/photos_total),  duration.seconds, duration_estimate_minutes)
      
      if 'checksum:md5=' in p.machine_tags:
          continue
      if p.media == 'photo':
          save_filename = '%s/%s.jpg' % (ps_dir, p.id)
          p.save(save_filename, 'Original')
      elif p.media == 'video':
          save_filename = '%s/%s.mp4' % (ps_dir, p.id)
          try:
              if p.video['failed'] == 1:
                  print "FAILED video file stub saved"
                  save_filename = '%s.failed' % save_filename
                  with open(save_filename, 'w') as f:
                      f.write('')
              else:
                  p.save(save_filename, 'Video Original')
          except Exception as e:
              import pdb; pdb.set_trace()
              print "hmm"
      else:
          import pdb; pdb.set_trace()
          print "Media type: %s" % p.media
      print 'Saved %s' % save_filename
      date_taken = int(datetime.datetime.strptime(p.datetaken, '%Y-%m-%d %H:%M:%S').strftime('%s'))
      print "date taken = %s" % date_taken
      os.utime(save_filename, (date_taken, date_taken))
      md5 = hashlib.md5(open(save_filename).read()).hexdigest()
      print md5
      p.addTags('checksum:md5=%s' % md5)
  # update directory date since it gets changed from writing files to it
  os.utime(ps_dir, (int(ps.date_create), int(ps.date_create)))
