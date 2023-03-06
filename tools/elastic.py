# ========================= class to communicate with elastic cloud ===================================
import requests, json
import pandas as pd
import numpy as np
from tools import various as var


class Elastic:
    APP_TYPES = {
        'kibana' : {
            'headers':{'Content-type': 'application/json', 'kbn-xsrf': 'true'},
            'dumps': lambda body: json.dumps(body)
        },
        'json' : {
            'headers':{'Content-type': 'application/json'},
            'dumps': lambda body: json.dumps(body)
        },
        'ndjson' : {
            'headers': {'Content-type': 'application/x-ndjson'},
            'dumps': lambda body: '\n'.join([json.dumps(el) for el in body]) + '\n'
        }
    }

    def __init__(self, config_file=None, url=None, auth=None):
        if config_file is None:
            self.base_url = url
            self.auth = auth
        else:
            with open(config_file) as json_file:
                config = json.load(json_file)
            self.base_url = config['url']
            self.auth = tuple(config['auth'])

    def url(self, path):
        return self.base_url + '/' + path.lstrip('/')

    def get(self, path, data=None, app_type='json'):
        url = self.url(path)
        headers = self.APP_TYPES[app_type]['headers']
        if data is None:
            r = requests.get(url, auth=self.auth)
        else:
            data = self.APP_TYPES[app_type]['dumps'](data)
            r = requests.get(url, data = data, auth=self.auth, headers=headers)
        return json.loads(r.text)

    def put(self, path, data=None, app_type='json'):
        url = self.url(path)
        headers = self.APP_TYPES[app_type]['headers']
        if data is None:
            r = requests.put(url, auth=self.auth)
        else:
            data = self.APP_TYPES[app_type]['dumps'](data)
            r = requests.put(url, data = data, auth=self.auth, headers=headers)
        return json.loads(r.text)

    def post(self, path, data, app_type = 'json'):
        url = self.url(path)
        headers = self.APP_TYPES[app_type]['headers']
        data = self.APP_TYPES[app_type]['dumps'](data)
        r = requests.post(url, data = data, auth=self.auth, headers=headers)
        return json.loads(r.text)

    def delete(self, path, app_type = 'json'):
        url = self.url(path)
        headers = self.APP_TYPES[app_type]['headers']
        r = requests.delete(url, auth=self.auth, headers=headers)
        if r.text == '':
            return ''
        else:
            return json.loads(r.text)

    def get_by_id(self,_id):
        data = {
            "query": {
                "ids" : {
                    "values" : [_id]
                }
            },
            "fields":["*"],
            "_source":True
        }
        return self.get("_search",data)


# ------------------- Index Class for index API ---------------------------------
URL = "https://f71a0894941b4c0389adafd64b548b9a.europe-west3.gcp.cloud.es.io"
AUTH = ("ingester","SmartBH75!")
io = Elastic(url = URL, auth = AUTH)


format = var.myformat
class Index:
    def __init__(self, index_template, io = io):
        self.io = io
        self.index_template = index_template

    def index(self):
        return format(self.index_template)

    def mappings(self):
        return self.io.get('{}/_mapping'.format(self.index()))

    def delete(self):
        return self.io.delete(self.index())

    def create(self, mappings = None):
        if mappings:
            return self.io.put(self.index(), {'mappings':mappings})
        else:
            return self.io.put(self.index())

    def update_mapping(self, mappings):
        return self.io.put('{}/_mapping'.format(self.index()), mappings)

    def reindex(self, new_index):
        data = {
            "source" : {"index": self.index()},
            "dest" : {"index":new_index}
        }
        return self.io.post('_reindex',data)

    def _prepare_doc(self, data, timefield='@timestamp', add_timefield=True):
        doc = {}
        for key, value in data.items():
            if np.any(pd.notnull(value)):   # ignore NaNs
                if type(value) == str:
                    try:
                        value = json.loads(value)  # try to make lists and dicts if possible
                    except:
                        pass
                if not var.myisempty(value):
                    doc[key] = value

        if add_timefield & (timefield not in doc.keys()):
            doc[timefield] = var.now2iso()

        return var.unflatdict(doc)

    def write(self, data, id_template=None, **kwargs):
        doc = self._prepare_doc(data,  **kwargs)
        if id_template:
            Id = format(id_template)
            return self.io.put(self.index()+'/_doc/'+Id, doc)
        else:
            return self.io.post(self.index()+'/_doc/', doc)

    def update(self, data, id_template, **kwargs):
        doc = self._prepare_doc(data,  **kwargs)
        doc = {'doc':doc}  # the simplest update
        Id = format(id_template)
        return self.io.post(self.index()+'/_update/'+Id, doc)

    def read(self, id_template, subfield=None, excludes=None):
        Id = format(id_template)
        if excludes is None:
            path = '{}/_doc/{}/_source'.format(self.index(), Id)
        else:
            path = '{}/_doc/{}/_source?_source_excludes={}'.format(self.index(), Id, excludes)

        doc = self.io.get(path)
        if ('error' in doc.keys()) & ('status' in doc.keys()):
            return doc
        elif subfield is None:
            return doc
        else:
            return doc[subfield]

    def search(self, query):
        return self.io.get('{}/_search'.format(self.index()),query)


# ------------------- Bulk Class for bulk & _search API &  ---------------------------------

class Bulk:
    def __init__(self, io = io, pipeline = None):
        self.io = io
        if pipeline is None:
            self.path = '/_bulk'
        else:
            self.path = '/_bulk?pipeline='+pipeline

    def _body(self, df, cmd, **kwargs):
        cmds = [{cmd:item} for item in df.filter(regex='^_').to_dict(orient='records')]
        docs = df.filter(regex='^(?!_)').to_dict(orient='records')
        # docs =[{k:v for k,v in m.items() if pd.notnull(v)} for m in docs]  # remove NaNs -> Could also be on the level of
        # index = Index('')
        docs = [Index('')._prepare_doc(doc, **kwargs) for doc in docs]
        if cmd == 'update':
            docs = [{"doc":doc} for doc in docs]
        body = []
        for t in zip(cmds, docs):
            body += list(t)
        return body

    def write(self, df, **kwargs):
        body = self._body(df,'index', **kwargs)
        return self.io.post(self.path, body, app_type='ndjson')

    def update(self, df, add_timefield=False, **kwargs):
        body = self._body(df,'update', add_timefield=add_timefield, **kwargs)
        return self.io.post(self.path, body, app_type='ndjson')

    def delete(self, df):
        # df need columns _index and _id
        body = [{'delete':item} for item in df.filter(regex='^_').to_dict(orient='records')]
        return self.io.post(self.path, body, app_type='ndjson')


    # --------- reading based on search -------------------

    def _result2df(self, res):
        DROP_COLS = ['_type','_score']
        if 'hits' in res:
            hits = res['hits']['hits']
        else:
            hits = res
        if len(hits) == 0:
            return pd.DataFrame()
        else:
            df = pd.DataFrame(hits).drop(columns=DROP_COLS)
            if '_source' in df.columns:
                source_df = pd.DataFrame(df['_source'].apply(lambda d: var.flatdict(d,list_flag=False)).values.tolist())
                df = pd.concat([df.drop(columns=['_source']), source_df],axis=1)
            if 'fields' in df.columns:
                fields_flag = pd.notnull(df.fields)
                fields_df = pd.DataFrame(df['fields'].dropna().values.tolist())
                fields_df.index = fields_flag[fields_flag].index
                for col in fields_df.columns:
                     df.loc[fields_flag,col] = fields_df[col]
                df.drop(columns=['fields'],inplace=True)
            return df

    def read_all_docs(self, target, size = 10):
        query = {
            "size" :size,
            "query": {
                "match_all": {}
            }
        }
        res = Index(target).search(query)
        return self._result2df(res)

    def read_latest_docs(self, target, unique_field, order_field, size = 10):
        search = {
            "size" :size,
            "query": {
                "match_all": {
                }
            },
            "collapse": {
                "field": unique_field,
                "inner_hits": {
                    "name": "latest",
                    "size": 1,
                    "sort": [ { order_field: "desc" } ]
                }
            }
        }
        res = Index(target).search(search)
        res = [item['inner_hits']['latest']['hits']['hits'][0] for item in res['hits']['hits']]
        return self._result2df(res)

    def query(self, target, match=None, term=None, size=10, fields=None, raw = False):
        # WARNING: fields does not yet work, as the output is different
        # see: https://www.elastic.co/guide/en/elasticsearch/reference/7.14/search-fields.html

        query = dict()
        match_all = True
        if match:
            query.update({'match':match})
            match_all = False

        if term:
            query.update({'term':term})
            match_all = False

        if match_all:
            query.update({'match_all':{}})

        search = {'size':size,'query':query}
        if fields:
            search.update({'fields':fields,'_source':False})

        res = Index(target).search(search)
        if raw:
            return res
        else:
            return self._result2df(res)

    def boolquery(self, target, termfilters={}, gtfilters={}, ltfilters={}, size=10):
        '''
        gtfilters: greater than filters
        ltfilrers: lower than filters
        
        https://www.elastic.co/guide/en/elasticsearch/reference/7.14/query-filter-context.html
        '''
         
        filter = list()
        for field, value in termfilters.items():
            filter.append({'term': {field:value}})
            
        for field, value in gtfilters.items():
            filter.append({'range': {field:{'gt':value}}})

        for field, value in ltfilters.items():
            filter.append({'range': {field:{'lt':value}}})

        search = {
            "size" : size,
            "query": {
                "bool": {
                    "filter": filter
                }
            }
        }
        res = Index(target).search(search)
        return self._result2df(res)


# ------------- IndexTemplate class for index template API ---------------
# https://www.elastic.co/guide/en/elasticsearch/reference/current/index-templates.html


class IndexTemplate:
    def __init__(self, name, io=io):
        self.name = name
        self.io = io

    def create(self, index_pattern_list, mappings):
        data = {
            'index_patterns': index_pattern_list,
            'template':{'mappings':mappings}
        }
        return self.io.put('_index_template/'+self.name, data)

    def get(self):
        return self.io.get('_index_template/'+self.name)

    def delete(self):
            return self.io.delete('_index_template/'+self.name)


# ----------- Class for pipelines ------------------------------------------
class Pipeline:
    def __init__(self, io=io, ignore_failure=True):
        self.io = io
        self.processors = []
        self.ignore_failure=ignore_failure

    def dot_expand(self, field='*'):
        processor = {
            "dot_expander" : {
                'ignore_failure': self.ignore_failure,
                'field': field
            }
        }
        self.processors.append(processor)
        return self

    def rename(self, field, target_field, expand = True):
        if ('.' in field) & expand:
            self.dot_expand(field)
        processor = {
            "rename": {
                'ignore_failure': self.ignore_failure,
                'field':field,
                'target_field':target_field
            }
        }
        self.processors.append(processor)
        return self

    def convert(self, field, typ):
        processor = {
            "convert": {
                'ignore_failure': self.ignore_failure,
                'field':field,
                'type':typ
            }
        }
        self.processors.append(processor)
        return self

    def script(self, source):
        processor = {
            "script": {
                'ignore_failure': self.ignore_failure,
                'source':source
            }
        }
        self.processors.append(processor)
        return self

    def remove(self, field, expand=True):
        if type(field)==str:
            field = [field]
        if expand:
            for el in field:
                if '.' in el:
                    self.dot_expand(el)
        processor = {
            "remove": {
                'ignore_failure': self.ignore_failure,
                'field':field
            }
        }
        self.processors.append(processor)
        return self

    def pipeline(self):
        return {'processors':self.processors}

    def pprint(self, indent=2):
        print(json.dumps(self.pipeline(),indent=indent))

    def simulate(self, doc):
        data = {
            'pipeline': self.pipeline(),
            'docs': [{'_source':doc}]
        }
        path = '_ingest/pipeline/_simulate'
        return self.io.post(path,data)

    def create(self, name):
        path = '_ingest/pipeline/' + name
        return self.io.put(path,self.pipeline())



# ----------- Class for users, roles tailored for customers -----------------
# users: info about the users and definition, which role they have
# roles: includes indices

class Role:
    def __init__(self, name, io = io):
        self.name = name
        self.io = io

    def get(self):
        res = self.io.get('/_security/role/' + self.name)
        if self.name in res.keys():
            return res[self.name]
        else:
            return res

    def post(self, role):
        '''
        Write all role data
        :param role: all data defining the role
        :return: result
        '''
        return self.io.post('/_security/role/' + self.name, role)


class Customer:
    ROLE_PATTERN = 'cu-{customer_id}'
    # INDEX_PATTERN = 'cu-{customer_id}-data'

    def __init__(self, username, io):
        '''
        Make a customer object
        :param username: new username
        :param io: must be root
        '''
        self.username = username
        self.io = io

    def create(self, customer_id,  password, index_pattern='cu-*', id_filter=True):
        '''
        Create new customer
        :param customer_id:  customer ID
        :param password: password
        :param index_pattern: DEFAULT = 'cu-*'
        :param id_filter: Whether customer can only see data with his customer_id
        :return: result
        '''
        role_id = self.ROLE_PATTERN.format(customer_id=customer_id)
        # index = self.INDEX_PATTERN.format(customer_id=customer_id)
        out = []
        role = {
            'indices': [{
                'names': [index_pattern],
                'privileges': ['read']
            }],
            'applications': [{
                'application': 'kibana-.kibana',
                'privileges': ['feature_dashboard.read'],
                'resources': ['space:products']
            }],
        }
        if id_filter:
            role['indices'][0]['query'] = {
                'term': { 'labels.customer_id': customer_id}
            }
        res = self.io.post('/_security/role/'+role_id,role)
        out.append(res)

        user = {
            'password': password,
            'roles': [role_id]
        }
        res = self.io.post('/_security/user/'+self.username,user)
        out.append(res)

        return out

    def get_user(self):
        return self.io.get('/_security/user/'+self.username)

    def get_role(self, role_id):
        return self.io.get('/_security/role/'+role_id)

    def get_roles(self):
        roles = self.get_user()[self.username]['roles']
        return [self.io.get('/_security/role/'+role) for role in roles]


# ==================== Kibana ================================================
class SavedObjects:
    def __init__(self, space_id, io):
        self.io = io
        self.root_path = '/s/{}/api/saved_objects/'.format(space_id)

    def get(self, space_id, obj_type, obj_id):
        path = '/s/{}/api/saved_objects/{}/{}'.format(space_id, obj_type, obj_id)
        return self.io.get(path, app_type='kibana')

    def export_dashboard(self,Id):
        path = self.root_path + '_export'
        data = {
            'excludeExportDetails' : True,
            'objects': [{
                "type": "dashboard",
                'id': Id
            }]
        }
        return self.io.post(path, data, app_type='kibana')

    def dashboards(self):
        path = self.root_path + '_find?type=dashboard&search_fields=title&search=*&fields=title'.format(type)
        res = self.io.get(path, app_type='kibana')
        data = [{'title':item['attributes']['title'], 'id':item['id']} for item  in res['saved_objects']]
        return pd.DataFrame(data)



class IndexPattern:
    def __init__(self, space_id, Id, io):
        self.io = io
        self.id = Id
        self.space_id = space_id

    def get(self):
        path = '/s/{}/api/index_patterns/index_pattern/{}'.format(self.space_id, self.id)
        return self.io.get(path, app_type='kibana')

    def delete(self):
        path = '/s/{}/api/index_patterns/index_pattern/{}'.format(self.space_id, self.id)
        return self.io.delete(path, app_type='kibana')

    def create(self, title, **kwargs):
        '''

        :param title:
        :param kwargs: timeFieldName, ...
        :return:
        '''
        index_pattern = {'title':title, 'id':self.id}
        index_pattern.update(kwargs)
        data = {
            'index_pattern':index_pattern
        }
        path = '/s/{}/api/index_patterns/index_pattern'.format(self.space_id)
        return self.io.post(path, data, app_type='kibana')

    def update(self, **kwargs):
        data = {
            'index_pattern':kwargs
        }
        path = '/s/{}/api/index_patterns/index_pattern/{}'.format(self.space_id, self.id)
        return self.io.post(path, data, app_type='kibana')

    def create_runtime_field(self, name, runtimeField):
        data = {
            "name": name,
            "runtimeField": runtimeField
        }
        path = '/s/{}/api/index_patterns/index_pattern/{}/runtime_field'.format(self.space_id, self.id)
        return self.io.post(path, data, app_type='kibana')





