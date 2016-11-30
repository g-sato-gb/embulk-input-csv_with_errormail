module Embulk
  module Input

    class CsvWithErrormail < InputPlugin
      Plugin.register_input("csv_with_errormail", self)

      def self.transaction(config, &control)
        task = {
          path_prefix: config.param('path_prefix', :string),
          charset: config.param('charset', :string, default: 'UTF-8'),
          newline: config.param('newline', :string, default: 'CRLF'),
          delimiter: config.param('delimiter', :string, default: ','),
          quote: config.param('quote', :string, default: '"'),
          escape: config.param('escape', :string, default: ''),
          null_string: config.param('null_string', :string, default: nil),
          header_line: config.param('header_line', :bool, default: true),
          delete_file: config.param('delete_file', :bool, default: true),
          columns: config.param('columns', :array, default: []),
          mailer: config.param('mailer', :hash, default: {}),
        }
        threads = config.param('threads', :integer, default: 1)
        columns = task[:columns].each_with_index.map do |c, i|
          Column.new(i, c["to_name"], c["type"].to_sym)
        end
        commit_reports = yield(task, columns, threads)
        return {}
      end

      # TODO
      #def self.guess(config)
      #  sample_records = [
      #    {"example"=>"a", "column"=>1, "value"=>0.1},
      #    {"example"=>"a", "column"=>2, "value"=>0.2},
      #  ]
      #  columns = Guess::SchemaGuess.from_hash_records(sample_records)
      #  return {"columns" => columns}
      #end

      def run
        log = Embulk::logger
        log.info("▼▼▼▼▼▼▼▼▼▼▼ input start 【csv_with_errormail】")
        task_report = {}
        begin
          Dir.glob(task[:path_prefix] + '*.csv').each {|f|
            log.info("input file : #{f}")
            next unless FileTest.file?(f)
            CSV.open(f, "rb:BOM|UTF-8", {headers: true, :col_sep => ",",:quote_char => '"' }){|csv|
             csv.read.each{|csv_row|
              @page_builder.add(make_record(@task[:columns], csv_row))
             }
            }
            if task[:delete_file]
              log.info("delete file : #{f}")
              File.delete f
            end
          }
          log.info("△△△△△△△△△△△ input end   【csv_with_errormail】")
          @page_builder.finish
        rescue => e
          log.error("!!!!!!!!!!!!error!!!!!!!!!!!! : #{e.message} \n" + e.backtrace.join("\n"))
          # エラーメール送信
          send_mail task['mailer'] do |mail|
            mail.body = PP.pp("#{e.message} \n",'')
          end
          log.info("==ERROR   END==")
        end
        return task_report
      end

      def make_record(columns, csv_row)
        columns.map do |column|
          val = csv_row[column["from_name"]]
          v = val.nil? ? "" : val
          type = column["type"]
          case type
            when "string"
              v
            when "long"
              v.to_i
            when "double"
              v.to_f
            when "boolean"
              if v.nil?
                nil
              elsif v.kind_of?(String)
                ["yes", "true", "1"].include?(v.downcase)
              elsif v.kind_of?(Numeric)
                !v.zero?
              else
                !!v
              end
            when "timestamp"
              v.empty? ? nil : Time.iso8601(v)
            else
              raise "Unsupported type #{type}"
          end
        end
      end

      def send_mail(param, &body_edit)
        mail = Mail.new
        param['mail'].each {|k,v| mail.send(k,v)}
        body_edit.call(mail) if body_edit
        mail.delivery_method :smtp, param['smtp'].map{|k,v| [k.to_sym, v] }.to_h
        mail.deliver!
      end

    end

  end
end