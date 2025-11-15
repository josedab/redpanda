// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include <seastar/core/sstring.hh>

#include <string_view>
#include <vector>

namespace redpanda::sql {

struct token {
    enum class type {
        // Keywords
        SELECT,
        FROM,
        WHERE,
        GROUP,
        BY,
        HAVING,
        ORDER,
        JOIN,
        INNER,
        LEFT,
        RIGHT,
        FULL,
        OUTER,
        ON,
        AS,
        CREATE,
        TABLE,
        STREAM,
        MATERIALIZED,
        VIEW,
        WITH,
        AND,
        OR,
        NOT,
        IN,
        LIKE,
        BETWEEN,
        IS,
        NULL_TOKEN,
        TRUE_TOKEN,
        FALSE_TOKEN,
        DISTINCT,
        COUNT,
        SUM,
        AVG,
        MIN,
        MAX,
        WINDOW,
        OVER,
        PARTITION,
        INTERVAL,
        TUMBLE,
        HOP,
        SESSION,

        // Operators
        PLUS,
        MINUS,
        STAR,
        SLASH,
        PERCENT,
        EQUAL,
        NOT_EQUAL,
        LESS,
        LESS_EQUAL,
        GREATER,
        GREATER_EQUAL,
        CONCAT,

        // Delimiters
        LEFT_PAREN,
        RIGHT_PAREN,
        LEFT_BRACKET,
        RIGHT_BRACKET,
        LEFT_BRACE,
        RIGHT_BRACE,
        COMMA,
        SEMICOLON,
        DOT,
        COLON,
        ARROW,

        // Literals
        INTEGER_LITERAL,
        FLOAT_LITERAL,
        STRING_LITERAL,
        IDENTIFIER,

        // Special
        END_OF_FILE,
        INVALID
    };

    type token_type;
    ss::sstring lexeme;
    size_t line;
    size_t column;

    token(type t, ss::sstring lex, size_t ln, size_t col)
      : token_type(t)
      , lexeme(std::move(lex))
      , line(ln)
      , column(col) {}
};

class token_stream {
public:
    explicit token_stream(std::vector<token> tokens)
      : _tokens(std::move(tokens))
      , _pos(0) {}

    const token& peek() const {
        if (_pos >= _tokens.size()) {
            static token eof{
              token::type::END_OF_FILE, "", 0, 0};
            return eof;
        }
        return _tokens[_pos];
    }

    const token& consume() {
        if (_pos >= _tokens.size()) {
            static token eof{
              token::type::END_OF_FILE, "", 0, 0};
            return eof;
        }
        return _tokens[_pos++];
    }

    bool is_at_end() const {
        return _pos >= _tokens.size()
               || _tokens[_pos].token_type == token::type::END_OF_FILE;
    }

    size_t position() const { return _pos; }
    void set_position(size_t pos) { _pos = pos; }

private:
    std::vector<token> _tokens;
    size_t _pos;
};

class tokenizer {
public:
    explicit tokenizer(std::string_view source)
      : _source(source)
      , _start(0)
      , _current(0)
      , _line(1)
      , _column(1) {}

    std::vector<token> tokenize();

private:
    bool is_at_end() const { return _current >= _source.length(); }
    char advance();
    char peek() const;
    char peek_next() const;
    bool match(char expected);

    void skip_whitespace();
    void skip_line_comment();
    void skip_block_comment();

    token scan_token();
    token make_token(token::type type);
    token identifier_or_keyword();
    token number();
    token string_literal();

    bool is_digit(char c) const { return c >= '0' && c <= '9'; }
    bool is_alpha(char c) const {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
    }
    bool is_alphanumeric(char c) const { return is_alpha(c) || is_digit(c); }

    token::type check_keyword(
      size_t start,
      size_t length,
      const char* rest,
      token::type type);
    token::type identifier_type();

    std::string_view _source;
    size_t _start;
    size_t _current;
    size_t _line;
    size_t _column;
};

} // namespace redpanda::sql
