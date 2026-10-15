"""PandaLive browse adapter ported from pure_live/pandalive_api.dart.
Source project: https://github.com/liuchuancong/pure_live (AGPL-3.0).
"""
from __future__ import annotations
from typing import Any
from urllib.parse import quote
from browse_platforms.base import LivePlatform, room_dict
from browse_platforms.common import Http, as_str, strip_tags
API='https://api.pandalive.co.kr'; ORIGIN='https://www.pandalive.co.kr'
UA='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'

def _card(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw,dict): return None
    uid=as_str(raw.get('userId',''))
    if not uid or str(raw.get('isLive','')).lower() not in ('true','1'): return None
    return room_dict(roomId=uid,title=strip_tags(raw.get('title','')),nickname=strip_tags(raw.get('userNick','')),avatar=as_str(raw.get('userImg','')),cover=as_str(raw.get('thumbUrl',raw.get('ivsThumbnail',''))),area=as_str(raw.get('category','')),watching=as_str(raw.get('user','')),isLive=True,platform='pandatv')

class PandaAdapter(LivePlatform):
    site='pandatv'; name='PandaTV'; logo='https://www.pandalive.co.kr/favicon.ico'
    def __init__(self,cookie:str='')->None:
        self.http=Http(); self.cookie=cookie
    def _headers(self,referer:str|None=None)->dict[str,str]:
        h={'origin':ORIGIN,'referer':referer or ORIGIN+'/','user-agent':UA}
        if self.cookie:h['cookie']=self.cookie
        return h
    async def get_categories(self)->list[dict[str,Any]]:
        # 照 douyin/pure_live 的自条目模式：单父级「热门」并把自身插为第一个 child，
        # 前端扁平网格渲染后可点击进入推荐流
        return [{'id':'hot','name':'热门','pic':'','children':[{'id':'hot','name':'热门','pic':'','parentId':'hot'}]}]
    async def get_category_rooms(self,parent_area_id:str,area_id:str,page:int,page_size:int)->dict[str,Any]:
        return await self.get_recommend_rooms(page,page_size)
    async def _page(self, *, page:int, size:int, order:str, keyword:str='')->dict[str,Any]:
        offset=(page-1)*size; form={'offset':str(offset),'limit':str(size),'orderBy':order}
        # 【P1·补】关键词 URL 编码后再进 Referer（与 douyin 同类问题：httpx 头只接受 latin-1，
        # 韓文/中文关键词直接拼会 UnicodeEncodeError；表单 searchVal 仍是 UTF-8 原文不受影响）
        ref=ORIGIN+'/live' if not keyword else f'{ORIGIN}/search/live?text={quote(keyword,safe="")}'
        if keyword:form['searchVal']=keyword
        data=await self.http.post_json(API+'/v1/live/index',data=form,headers=self._headers(ref))
        if not isinstance(data,dict) or data.get('result') is not True: raise ValueError('PandaTV directory request rejected')
        paging=data.get('page') or {}; rows=data.get('list') if isinstance(data.get('list'),list) else []
        expected={'offset':str(offset),'limit':str(size),'page':str(page)}
        for key,value in expected.items():
            if str(paging.get(key,''))!=value: raise ValueError('PandaTV paging identity mismatch')
        total=int(paging.get('total') or 0)
        rooms=[value for value in (_card(row) for row in rows) if value]
        return {'list':rooms,'hasMore':offset+len(rows)<total}
    async def get_recommend_rooms(self,page:int,page_size:int)->dict[str,Any]:
        return await self._page(page=page,size=page_size,order='hot')
    async def search_rooms(self,keyword:str,page:int,page_size:int)->dict[str,Any]:
        return await self._page(page=page,size=min(page_size,50),order='user',keyword=keyword.strip())
    async def get_room_detail(self,room_id:str)->dict[str,Any]|None:
        data=await self.http.post_json(API+'/v1/member/bj',data={'userId':room_id,'info':'media fanGrade'},headers=self._headers(f'{ORIGIN}/{room_id}'))
        if not isinstance(data,dict) or data.get('result') is not True:return None
        profile=data.get('bjInfo') or {}
        media=data.get('media') if isinstance(data.get('media'),dict) else {}
        # 人气照 pure_live pandalive：media['user'] 为在线观众数
        watching=as_str(media.get('user'))
        return room_dict(roomId=as_str(profile.get('id',room_id)),title=as_str(profile.get('nick','')),nickname=as_str(profile.get('nick','')),avatar=as_str(profile.get('profileImg',profile.get('thumbUrl',''))),cover='',watching=watching,isLive=bool(data.get('media')),platform=self.site)
    async def close(self)->None:
        await self.http.close()
