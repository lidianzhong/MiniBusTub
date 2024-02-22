#include <iostream>
#include <string>
#include "binder/binder.h"
#include "common/bustub_instance.h"
#include "common/exception.h"
#include "common/util/string_util.h"
#include "libfort/lib/fort.hpp"
#include "linenoise/linenoise.h"
#include "utf8proc/utf8proc.h"

// 计算 UTF-8 字符串的宽度, 可能用来确定字符在终端中的显示宽度
auto GetWidthOfUtf8(const void *beg, const void *end, size_t *width) -> int {
  size_t computed_width = 0;
  utf8proc_ssize_t n;
  utf8proc_ssize_t size = static_cast<const char *>(end) - static_cast<const char *>(beg);
  auto pstring = static_cast<utf8proc_uint8_t const *>(beg);
  utf8proc_int32_t data;
  while ((n = utf8proc_iterate(pstring, size, &data)) > 0) {
    computed_width += utf8proc_charwidth(data);
    pstring += n;
    size -= n;
  }
  *width = computed_width;
  return 0;
}

/**
 *
 *
 * @param argc 表示传入参数的个数
 * @param argv 参数的数组
 * @return
 */
// NOLINTNEXTLINE
auto main(int argc, char **argv) -> int {
  // 设置 UTF-8 字符串的宽度
  ft_set_u8strwid_func(&GetWidthOfUtf8);

  // 创建数据库系统实例
  auto bustub = std::make_unique<bustub::BustubInstance>("test.db");

  // 定义了一些默认变量 默认提示符 emoji 提示符 是否使用表情提示符 是否禁用终端消息
  auto default_prompt = "minibustub> ";
  auto emoji_prompt = "\U0001f6c1> ";  // the bathtub emoji
  bool use_emoji_prompt = false;
  bool disable_tty = false;

  // 在命令行参数中查找 --emoji-prompt 和 --disable-tty，如果找到则相应地设置标志
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "--emoji-prompt") == 0) {
      use_emoji_prompt = true;
      break;
    }
    if (strcmp(argv[i], "--disable-tty") == 0) {
      disable_tty = true;
      break;
    }
  }

  // 生成模拟表格的数据调用
  bustub->GenerateMockTable();

  if (bustub->buffer_pool_manager_ != nullptr) {
    bustub->GenerateTestTable();
  }

  std::cout << "Welcome to the MiniBusTub shell! Type \\help to learn more." << std::endl << std::endl;

  // 设置 linenoise 库的历史记录最大长度和多行模式
  linenoiseHistorySetMaxLen(1024);
  linenoiseSetMultiLine(1);

  // 根据之前设置的标志选择要显示的提示符
  auto prompt = use_emoji_prompt ? emoji_prompt : default_prompt;

  while (true) {
    std::string query;
    bool first_line = true;
    while (true) {
      auto line_prompt = first_line ? prompt : "... ";
      if (!disable_tty) {
        // 读取用户输入
        char *query_c_str = linenoise(line_prompt);
        if (query_c_str == nullptr) {
          return 0;
        }
        query += query_c_str;
        if (bustub::StringUtil::EndsWith(query, ";") || bustub::StringUtil::StartsWith(query, "\\")) {
          break;
        }
        query += " ";
        linenoiseFree(query_c_str);
      } else {
        std::string query_line;
        std::cout << line_prompt;
        std::getline(std::cin, query_line);
        if (!std::cin) {
          return 0;
        }
        query += query_line;
        if (bustub::StringUtil::EndsWith(query, ";") || bustub::StringUtil::StartsWith(query, "\\")) {
          break;
        }
        query += "\n";
      }
      first_line = false;
    }

    if (!disable_tty) {
      linenoiseHistoryAdd(query.c_str());
    }

    try {
      auto writer = bustub::FortTableWriter();
      // 执行开始，记录时间
      auto start_time = std::chrono::high_resolution_clock::now();
      bustub->ExecuteSql(query, writer);
      // 执行结束，记录时间
      auto end_time = std::chrono::high_resolution_clock::now();
      // 计算执行的时间差
      auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
      for (const auto &table : writer.tables_) {
          std::cout << table;
      }
      std::cout << "Execution time: " << duration.count() << " microseconds" << std::endl;
    } catch (bustub::Exception &ex) {
      std::cerr << ex.what() << std::endl;
    }
  }

  return 0;
}
