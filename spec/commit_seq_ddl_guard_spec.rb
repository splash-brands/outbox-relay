# frozen_string_literal: true

require 'spec_helper'

# Always-on regression guard (runs on the default SQLite suite, no PostgreSQL
# needed) for the 0.10.0 bug: the commit_seq assignment trigger was generated as a
# CONSTRAINT TRIGGER with `REFERENCING NEW TABLE` + `FOR EACH STATEMENT`, which
# PostgreSQL rejects (`syntax error at or near "REFERENCING"`). A constraint
# trigger must be FOR EACH ROW and may not use transition tables. This guards every
# template that ships the trigger so the bad form can never silently return.
RSpec.describe 'commit_seq trigger DDL templates (SB-2140 regression guard)' do
  root = File.expand_path('..', __dir__)

  templates = {
    'add_commit_seq generator' =>
      'lib/generators/outbox_relay/add_commit_seq/templates/add_commit_seq.rb.erb',
    'harden_commit_seq generator' =>
      'lib/generators/outbox_relay/harden_commit_seq/templates/harden_commit_seq.rb.erb',
    'install generator' =>
      'lib/generators/outbox_relay/install/templates/create_outbox_relay_tables.rb.erb'
  }

  templates.each do |label, rel|
    context label do
      let(:sql) { File.read(File.join(root, rel)) }

      it 'declares its CONSTRAINT TRIGGER as FOR EACH ROW' do
        expect(sql).to match(/CONSTRAINT TRIGGER/)
        expect(sql).to match(/FOR EACH ROW/)
      end

      it 'never combines CONSTRAINT TRIGGER with transition tables or FOR EACH STATEMENT' do
        expect(sql).not_to match(/REFERENCING\s+NEW\s+TABLE/i)
        expect(sql).not_to match(/FOR EACH STATEMENT/i)
      end
    end
  end
end
