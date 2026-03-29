import os
from datetime import datetime, date
from loguru import logger

from apis.xhs_pc_apis import XHS_Apis
from xhs_utils.common_util import init, load_mysql_config
from xhs_utils.data_util import handle_note_info, handle_comment_info, download_note, save_to_xlsx
from xhs_utils.mysql_util import save_notes_and_comments_to_mysql


class Data_Spider:
    def __init__(self):
        self.xhs_apis = XHS_Apis()

    def spider_note(self, note_url: str, cookies_str: str, proxies=None):
        note_info = None
        try:
            success, msg, note_info = self.xhs_apis.get_note_info(note_url, cookies_str, proxies)
            if success:
                note_info = note_info['data']['items'][0]
                note_info['url'] = note_url
                note_info = handle_note_info(note_info)
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取笔记信息 {note_url}: {success}, msg: {msg}')
        return success, msg, note_info

    def spider_note_comments(self, note_url: str, cookies_str: str, proxies=None):
        comments = []
        try:
            success, msg, raw_comments = self.xhs_apis.get_note_all_comment(note_url, cookies_str, proxies)
            if not success:
                return success, msg, comments

            for out_comment in raw_comments:
                out_comment['note_url'] = note_url
                comments.append(handle_comment_info(out_comment))
                for inner_comment in out_comment.get('sub_comments', []):
                    inner_comment['note_id'] = out_comment['note_id']
                    inner_comment['note_url'] = note_url
                    comments.append(handle_comment_info(inner_comment))
        except Exception as e:
            success = False
            msg = e
        logger.info(f'爬取评论 {note_url}: {success}, msg: {msg}, 评论数: {len(comments)}')
        return success, msg, comments

    @staticmethod
    def _parse_upload_date(upload_time: str):
        return datetime.strptime(upload_time, '%Y-%m-%d %H:%M:%S').date()

    @staticmethod
    def _date_label(start_date: date, end_date: date):
        if start_date == end_date:
            return start_date.strftime('%Y-%m-%d')
        if start_date.year == end_date.year and start_date.month == end_date.month:
            return start_date.strftime('%Y-%m')
        return f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"

    def spider_topic_notes(
        self,
        keywords: list,
        start_date: str,
        end_date: str,
        cookies_str: str,
        base_path: dict,
        per_keyword_search_num: int = 80,
        max_result_num: int = 100,
        include_comments: bool = True,
        save_choice: str = 'mysql',
        mysql_config: dict = None,
        proxies=None,
    ):
        """
        按多个关键词搜索（OR），按时间范围过滤后按热度（点赞数）排序。
        save_choice: mysql / excel / all
        """
        start_day = datetime.strptime(start_date, '%Y-%m-%d').date()
        end_day = datetime.strptime(end_date, '%Y-%m-%d').date()
        if end_day < start_day:
            raise ValueError('end_date 不能早于 start_date')

        candidate_urls = []
        for keyword in keywords:
            success, msg, notes = self.xhs_apis.search_some_note(
                query=keyword,
                require_num=per_keyword_search_num,
                cookies_str=cookies_str,
                sort_type_choice=2,
                note_type=0,
                note_time=0,
                note_range=0,
                pos_distance=0,
                geo=None,
                proxies=proxies,
            )
            if not success:
                logger.warning(f"关键词 {keyword} 搜索失败: {msg}")
                continue
            note_items = [n for n in notes if n.get('model_type') == 'note']
            for note in note_items:
                candidate_urls.append(f"https://www.xiaohongshu.com/explore/{note['id']}?xsec_token={note['xsec_token']}")

        dedup_urls = list(dict.fromkeys(candidate_urls))
        logger.info(f'候选笔记数量（去重后）: {len(dedup_urls)}')

        note_list = []
        comment_list = []
        lowered_keywords = [k.strip().lower() for k in keywords if k.strip()]

        for note_url in dedup_urls:
            success, msg, note_info = self.spider_note(note_url, cookies_str, proxies)
            if not success or not note_info:
                continue

            text_body = f"{note_info.get('title', '')} {note_info.get('desc', '')}".lower()
            keyword_hits = [k for k in lowered_keywords if k in text_body]
            if not keyword_hits:
                continue

            upload_day = self._parse_upload_date(note_info['upload_time'])
            if not (start_day <= upload_day <= end_day):
                continue

            note_info['keyword_hits'] = keyword_hits
            note_list.append(note_info)

            if include_comments:
                c_success, c_msg, comments = self.spider_note_comments(note_url, cookies_str, proxies)
                if c_success:
                    comment_list.extend(comments)
                else:
                    logger.warning(f"评论抓取失败 {note_url}: {c_msg}")

        note_list.sort(key=lambda n: int(n.get('liked_count', 0)), reverse=True)
        if len(note_list) > max_result_num:
            note_list = note_list[:max_result_num]
            note_ids = {n['note_id'] for n in note_list}
            comment_list = [c for c in comment_list if c['note_id'] in note_ids]

        label = self._date_label(start_day, end_day)
        excel_name = f'通胀预期_{label}'

        if save_choice in ['excel', 'all']:
            file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}.xlsx'))
            save_to_xlsx(note_list, file_path, type='note')
            if include_comments and comment_list:
                comment_file_path = os.path.abspath(os.path.join(base_path['excel'], f'{excel_name}_评论.xlsx'))
                save_to_xlsx(comment_list, comment_file_path, type='comment')

        if save_choice in ['mysql', 'all']:
            if not mysql_config:
                raise ValueError('save_choice 为 mysql/all 时，mysql_config 不能为空')
            save_notes_and_comments_to_mysql(note_list, comment_list, mysql_config)

        if save_choice in ['media', 'all']:
            for note_info in note_list:
                download_note(note_info, base_path['media'], save_choice)

        logger.info(f'最终入库/导出笔记数: {len(note_list)}, 评论数: {len(comment_list)}')
        return note_list, comment_list


if __name__ == '__main__':
    cookies_str, base_path = init()
    mysql_config = load_mysql_config()
    data_spider = Data_Spider()

    # 通胀预期相关关键词（满足任意一个即可）
    keywords = [
        '通胀预期', 'CPI', 'PPI', '通货膨胀', '美国通胀', '核心CPI',
        '降息预期', '加息预期', '美联储', '实际利率', '通缩预期'
    ]

    # 时间上下界：支持按天/按月（按月可传该月 1 日到该月最后一日）
    start_date = '2026-03-01'
    end_date = '2026-03-31'

    data_spider.spider_topic_notes(
        keywords=keywords,
        start_date=start_date,
        end_date=end_date,
        cookies_str=cookies_str,
        base_path=base_path,
        per_keyword_search_num=80,
        max_result_num=100,
        include_comments=True,
        save_choice='mysql',  # mysql / excel / all / media
        mysql_config=mysql_config,
        proxies=None,
    )
